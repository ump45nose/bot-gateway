from contextlib import contextmanager
import json
from pathlib import Path
import sqlite3
import time


class Store:
    def __init__(self, root):
        self.root = Path(root)
        self.root.mkdir(mode=0o700, parents=True, exist_ok=True)
        self.path = self.root / 'gateway.sqlite3'
        with self.db() as db:
            db.execute('PRAGMA journal_mode=WAL')
            db.executescript('''
            CREATE TABLE IF NOT EXISTS bots (
              name TEXT PRIMARY KEY,cursor INTEGER NOT NULL DEFAULT 0,active INTEGER NOT NULL DEFAULT 0,
              initialized INTEGER NOT NULL DEFAULT 0,identity TEXT,poll_at REAL NOT NULL DEFAULT 0,
              received_at REAL NOT NULL DEFAULT 0,error TEXT,pause_until REAL NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS updates (
              bot TEXT NOT NULL,id INTEGER NOT NULL,body TEXT NOT NULL,received REAL NOT NULL,
              binding TEXT,reason TEXT,PRIMARY KEY(bot,id));
            CREATE TABLE IF NOT EXISTS deliveries (
              binding TEXT NOT NULL,bot TEXT NOT NULL,update_id INTEGER NOT NULL,
              status TEXT NOT NULL DEFAULT 'pending',attempts INTEGER NOT NULL DEFAULT 0,
              next_run REAL NOT NULL DEFAULT 0,error TEXT,confirmed REAL,
              PRIMARY KEY(binding,bot,update_id));
            CREATE INDEX IF NOT EXISTS deliveries_due ON deliveries(binding,status,next_run,update_id);
            CREATE TABLE IF NOT EXISTS messages (
              bot TEXT NOT NULL,chat TEXT NOT NULL,id INTEGER NOT NULL,binding TEXT NOT NULL,
              thread INTEGER NOT NULL DEFAULT 0,PRIMARY KEY(bot,chat,id));
            CREATE TABLE IF NOT EXISTS callbacks (
              bot TEXT NOT NULL,id TEXT NOT NULL,binding TEXT NOT NULL,chat TEXT NOT NULL,
              thread INTEGER NOT NULL DEFAULT 0,acked INTEGER NOT NULL DEFAULT 0,
              PRIMARY KEY(bot,id));
            CREATE TABLE IF NOT EXISTS files (
              bot TEXT NOT NULL,binding TEXT NOT NULL,id TEXT NOT NULL,path TEXT,
              PRIMARY KEY(bot,binding,id));
            CREATE TABLE IF NOT EXISTS conversations (
              bot TEXT NOT NULL,user INTEGER NOT NULL,chat TEXT NOT NULL,thread INTEGER NOT NULL,
              binding TEXT NOT NULL,expires REAL NOT NULL,PRIMARY KEY(bot,user,chat,thread));
            CREATE TABLE IF NOT EXISTS contexts (
              binding TEXT NOT NULL,chat TEXT NOT NULL,id INTEGER NOT NULL,thread INTEGER NOT NULL,
              observed REAL NOT NULL,PRIMARY KEY(binding,chat,id));
            CREATE TABLE IF NOT EXISTS consumers (
              binding TEXT PRIMARY KEY,poll_at REAL NOT NULL DEFAULT 0,offset INTEGER NOT NULL DEFAULT 0);
            CREATE TABLE IF NOT EXISTS outbound (
              id TEXT PRIMARY KEY,binding TEXT NOT NULL,method TEXT NOT NULL,status TEXT NOT NULL,
              created REAL NOT NULL,finished REAL,error TEXT,chat TEXT,thread INTEGER,
              message_id INTEGER,fingerprint TEXT,response TEXT);
            CREATE TABLE IF NOT EXISTS commands (
              binding TEXT NOT NULL,scope TEXT NOT NULL,language TEXT NOT NULL,
              body TEXT NOT NULL,PRIMARY KEY(binding,scope,language));
            PRAGMA user_version=1;
            ''')
        self.path.chmod(0o600)

    @contextmanager
    def db(self, *, immediate=False):
        connection = sqlite3.connect(self.path, timeout=15)
        connection.row_factory = sqlite3.Row
        try:
            if immediate:
                connection.execute('BEGIN IMMEDIATE')
            yield connection
            connection.commit()
        except BaseException:
            connection.rollback()
            raise
        finally:
            connection.close()

    def bot(self, name):
        with self.db() as db:
            return dict(db.execute('SELECT * FROM bots WHERE name=?', (name,)).fetchone())

    def set_bot(self, name, **values):
        assert values.keys() <= {'cursor', 'active', 'initialized', 'identity', 'poll_at', 'received_at', 'error', 'pause_until'}
        with self.db() as db:
            db.execute('UPDATE bots SET ' + ','.join(k+'=?' for k in values) + ' WHERE name=?', (*values.values(), name))

    def owner(self, bot, chat, message_id):
        with self.db() as db:
            row = db.execute('SELECT binding FROM messages WHERE bot=? AND chat=? AND id=?',
                             (bot, str(chat), message_id)).fetchone()
            return row[0] if row else None

    def ingest(self, bot, update, binding, reason, context, *, lease_seconds=0):
        now, update_id = time.time(), update['update_id']
        with self.db(immediate=True) as db:
            inserted = db.execute('INSERT OR IGNORE INTO updates VALUES(?,?,?,?,?,?)',
                (bot, update_id, json.dumps(update, ensure_ascii=False), now, binding, reason)).rowcount
            if inserted and binding:
                db.execute('INSERT INTO deliveries(binding,bot,update_id) VALUES(?,?,?)', (binding, bot, update_id))
                if context.get('callback_id'):
                    db.execute('INSERT OR IGNORE INTO callbacks(bot,id,binding,chat,thread) VALUES(?,?,?,?,?)',
                        (bot, context['callback_id'], binding, str(context['chat']), context['thread']))
                self.remember_files(db, bot, binding, update)
                if lease_seconds:
                    db.execute('INSERT OR REPLACE INTO conversations VALUES(?,?,?,?,?,?)',
                        (bot, context['user'], str(context['chat']), context['thread'], binding, now+lease_seconds))
                elif reason == 'conversation_route':
                    db.execute('DELETE FROM conversations WHERE bot=? AND user=? AND chat=? AND thread=?',
                        (bot, context['user'], str(context['chat']), context['thread']))
            db.execute('UPDATE bots SET cursor=MAX(cursor,?),received_at=?,error=NULL WHERE name=?',
                       (update_id+1, now, bot))

    def poll(self, binding, offset, limit, *, ack_on_delivery=False):
        with self.db(immediate=True) as db:
            db.execute('INSERT OR REPLACE INTO consumers VALUES(?,?,?)', (binding, time.time(), offset))
            if offset > 0:
                db.execute("UPDATE deliveries SET status='delivered',confirmed=? WHERE binding=? AND update_id<? AND status='pending'",
                           (time.time(), binding, offset))
            elif offset < 0:
                ids = db.execute("SELECT update_id FROM deliveries WHERE binding=? AND status='pending' ORDER BY update_id DESC LIMIT ?",
                                 (binding, -offset)).fetchall()
                if ids:
                    db.execute("UPDATE deliveries SET status='delivered',confirmed=? WHERE binding=? AND update_id<? AND status='pending'",
                               (time.time(), binding, min(row[0] for row in ids)))
            rows = db.execute('''SELECT u.body FROM deliveries d JOIN updates u ON u.bot=d.bot AND u.id=d.update_id
                WHERE d.binding=? AND d.status='pending' ORDER BY d.update_id LIMIT ?''', (binding, limit)).fetchall()
            updates = [json.loads(row[0]) for row in rows]
            if ack_on_delivery:
                # Legacy remote-control scripts can restart themselves before sending their next offset.
                # Their destructive actions must not be automatically replayed after a restart.
                for update in updates:
                    db.execute("UPDATE deliveries SET status='delivered',confirmed=? WHERE binding=? AND update_id=?",
                               (time.time(), binding, update['update_id']))
            from .router import context
            for update in updates:
                ctx = context(update)
                if ctx['message_id'] is not None:
                    db.execute('INSERT OR REPLACE INTO contexts VALUES(?,?,?,?,?)',
                        (binding, str(ctx['chat']), ctx['message_id'], ctx['thread'], time.time()))
            return updates

    def due(self, binding):
        with self.db() as db:
            row = db.execute('''SELECT d.*,u.body FROM deliveries d JOIN updates u ON u.bot=d.bot AND u.id=d.update_id
                WHERE d.binding=? AND d.status='pending' AND d.next_run<=? ORDER BY d.update_id LIMIT 1''',
                (binding, time.time())).fetchone()
            return dict(row) if row else None

    def delivery(self, row, *, accepted=False, error=None, permanent=False, delay=0):
        with self.db() as db:
            db.execute('''UPDATE deliveries SET status=?,attempts=attempts+1,error=?,next_run=?,confirmed=?
                WHERE binding=? AND bot=? AND update_id=?''',
                ('delivered' if accepted else 'failed' if permanent else 'pending', error,
                 time.time()+delay, time.time() if accepted else None, row['binding'], row['bot'], row['update_id']))

    def remember(self, bot, binding, result):
        messages = result if isinstance(result, list) else [result]
        with self.db() as db:
            self.remember_files(db, bot, binding, result)
            for message in messages:
                if isinstance(message, dict) and isinstance(message.get('chat'), dict) and message.get('message_id'):
                    db.execute('INSERT OR REPLACE INTO messages VALUES(?,?,?,?,?)',
                        (bot, str(message['chat']['id']), message['message_id'], binding, message.get('message_thread_id', 0)))

    @staticmethod
    def remember_files(db, bot, binding, value):
        if isinstance(value, dict):
            if isinstance(value.get('file_id'), str):
                db.execute('INSERT OR IGNORE INTO files(bot,binding,id) VALUES(?,?,?)', (bot, binding, value['file_id']))
            for child in value.values():
                Store.remember_files(db, bot, binding, child)
        elif isinstance(value, list):
            for child in value:
                Store.remember_files(db, bot, binding, child)

    def conversation(self, bot, ctx):
        with self.db() as db:
            row = db.execute('SELECT binding FROM conversations WHERE bot=? AND user=? AND chat=? AND thread=? AND expires>?',
                (bot, ctx['user'], str(ctx['chat']), ctx['thread'], time.time())).fetchone()
            return row[0] if row else None

    def reply_thread(self, binding, params):
        if not params.get('chat_id'):
            return None
        reply = params.get('reply_parameters', {})
        if isinstance(reply, str):
            reply = json.loads(reply)
        reply_id = params.get('reply_to_message_id') or reply.get('message_id')
        with self.db() as db:
            if reply_id:
                row = db.execute('SELECT thread FROM contexts WHERE binding=? AND chat=? AND id=?',
                    (binding['name'], str(params['chat_id']), int(reply_id))).fetchone()
                if row:
                    return row['thread']
            mode = binding.get('infer_thread', '')
            if mode:
                rows = db.execute('SELECT thread FROM contexts WHERE binding=? AND chat=? AND observed>? ORDER BY observed DESC',
                    (binding['name'], str(params['chat_id']), time.time()-600)).fetchall()
                if rows and (mode == 'serial' or len({row['thread'] for row in rows}) == 1):
                    return rows[0]['thread']
                if rows:
                    raise ValueError('message_thread_id_required')
        return None

    def file_allowed(self, binding, *, file_id=None, path=None):
        with self.db() as db:
            row = db.execute('SELECT 1 FROM files WHERE bot=? AND binding=? AND ' + ('id=?' if file_id else 'path=?'),
                             (binding['bot'], binding['name'], file_id or path)).fetchone()
            return row is not None

    def status(self):
        with self.db() as db:
            return {'bots': [{k: row[k] for k in ('name', 'active', 'initialized', 'poll_at', 'received_at', 'error', 'pause_until')}
                             for row in db.execute('SELECT * FROM bots ORDER BY name')],
                    'deliveries': [dict(row) for row in db.execute('SELECT binding,status,COUNT(*) count FROM deliveries GROUP BY binding,status')],
                    'ignored': [dict(row) for row in db.execute('SELECT reason,COUNT(*) count FROM updates WHERE binding IS NULL GROUP BY reason')],
                    'outbound': [dict(row) for row in db.execute('SELECT binding,status,COUNT(*) count FROM outbound GROUP BY binding,status')],
                    'consumers': [dict(row) for row in db.execute('SELECT * FROM consumers ORDER BY binding')],
                    'recent_updates': [dict(row) for row in db.execute('SELECT bot,id,binding,reason,received FROM updates ORDER BY received DESC LIMIT 20')],
                    'recent_deliveries': [dict(row) for row in db.execute('SELECT binding,bot,update_id,status,attempts,error,confirmed FROM deliveries ORDER BY rowid DESC LIMIT 20')],
                    'recent_outbound': [dict(row) for row in db.execute('SELECT id,binding,method,status,created,finished,error,message_id FROM outbound ORDER BY created DESC LIMIT 20')]}
