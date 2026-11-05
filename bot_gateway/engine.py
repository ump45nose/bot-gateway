import hashlib
import hmac
import json
import logging
import re
import threading
import time
import uuid

import requests

from .router import choose
from .store import Store
from .upstream import Upstream

log = logging.getLogger('bot-gateway')
READ = {'getMe', 'getFile', 'getChat', 'getChatMember', 'getChatAdministrators', 'getUserProfilePhotos'}
SEND = {'sendMessage', 'sendPhoto', 'sendVideo', 'sendDocument', 'sendAudio', 'sendVoice', 'sendAnimation',
        'sendVideoNote', 'sendSticker', 'sendMediaGroup', 'sendLocation', 'sendContact', 'sendPoll',
        'sendChatAction', 'editMessageText', 'editMessageCaption', 'editMessageMedia', 'editMessageReplyMarkup',
        'deleteMessage', 'forwardMessage', 'copyMessage', 'answerCallbackQuery'}
TOPICS = {'createForumTopic', 'editForumTopic', 'closeForumTopic', 'reopenForumTopic', 'deleteForumTopic',
          'unpinAllForumTopicMessages'}


def failure(code, description):
    return code, {'ok': False, 'error_code': code, 'description': description}


class Gateway:
    def __init__(self, cfg):
        self.cfg, self.store, self.upstream = cfg, Store(cfg['state_dir']), Upstream(cfg)
        self.stop = threading.Event()
        self.changed = threading.Condition()
        self.poll_locks = {name: threading.Lock() for name in cfg['bindings']}
        self.bot_locks = {name: threading.Lock() for name in cfg['bots']}
        self.command_lock = threading.Lock()
        self.threads = []
        self.menu_dirty = {name: True for name in cfg['bots']}
        self.credentials = {b['credential_hash']: b for b in cfg['bindings'].values()}
        with self.store.db() as db:
            for name, bot in cfg['bots'].items():
                db.execute('INSERT OR IGNORE INTO bots(name,active) VALUES(?,?)', (name, int(bot['initially_active'])))

    def authenticate(self, credential):
        binding = self.credentials.get(hashlib.sha256(credential.encode()).hexdigest())
        return binding if binding and hmac.compare_digest(credential, binding['credential']) else None

    def identity(self, bot):
        status, response = self.upstream.api(bot, 'getMe', timeout=12)
        if not response.get('ok') or response.get('result', {}).get('id') != self.cfg['bots'][bot]['id']:
            self.store.set_bot(bot, error='bot_identity_unavailable')
            return False
        self.store.set_bot(bot, identity=json.dumps(response['result']))
        return True

    def activate(self, bot, active):
        if bot not in self.cfg['bots'] or type(active) is not bool:
            return failure(400, 'invalid_bot_activation')
        with self.bot_locks[bot]:
            state = self.store.bot(bot)
            if active and not state['initialized']:
                # Bootstrap once after old receivers have stopped. Previous user messages are never replayed.
                status, result = self.upstream.api(bot, 'getUpdates', {'offset': -1, 'timeout': 0,
                    'allowed_updates': ['message', 'callback_query']}, timeout=10)
                if not result.get('ok'):
                    return status, result
                cursor = max((u['update_id']+1 for u in result['result']), default=0)
                self.store.set_bot(bot, cursor=cursor, initialized=1)
            self.store.set_bot(bot, active=int(active), error=None)
        return 200, {'ok': True, 'result': {'bot': bot, 'active': active}}

    def start(self):
        def spawn(name, function, *args):
            thread = threading.Thread(target=function, args=args, name=name, daemon=True)
            self.threads.append(thread)
            thread.start()
        for bot in self.cfg['bots']:
            spawn('poll-' + bot, self.receive, bot)
        for name, binding in self.cfg['bindings'].items():
            if self.cfg['apps'][binding['app']]['mode'] == 'webhook':
                spawn('deliver-' + name, self.deliver, name)
        spawn('menus', self.menus)

    def receive(self, bot):
        delay = 1
        while not self.stop.is_set():
            try:
                state = self.store.bot(bot)
                if not state['identity'] and not self.identity(bot):
                    self.stop.wait(10)
                    continue
                if not state['active']:
                    self.stop.wait(1)
                    continue
                if not state['initialized']:
                    status, result = self.activate(bot, True)
                    if not result.get('ok'):
                        self.stop.wait(5)
                    continue
                with self.bot_locks[bot]:
                    status, result = self.upstream.api(bot, 'getUpdates', {'offset': state['cursor'],
                        'timeout': 20, 'allowed_updates': ['message', 'callback_query']}, timeout=30)
                self.store.set_bot(bot, poll_at=time.time())
                if not result.get('ok'):
                    code = result.get('error_code', status)
                    error = 'receiver_conflict' if code == 409 else 'telegram_rate_limited' if code == 429 else 'telegram_unavailable'
                    self.store.set_bot(bot, error=error)
                    log.warning('bot=%s receive=%s', bot, error)
                    wait = result.get('parameters', {}).get('retry_after', delay)
                    self.stop.wait(min(300, max(1, int(wait))))
                    delay = min(60, delay * 2)
                    continue
                delay = 1
                self.store.set_bot(bot, error=None)
                for update in result.get('result', []):
                    if type(update.get('update_id')) is not int:
                        raise ValueError('invalid_update')
                    binding, reason, ctx = choose(self.cfg, self.store, bot, update)
                    selected = self.cfg['bindings'].get(binding, {})
                    lease = 600 if ctx['data'] in selected.get('conversation_callbacks', []) else 0
                    self.store.ingest(bot, update, binding, reason, ctx, lease_seconds=lease)
                    if ctx.get('callback_id') and binding:
                        self.ack_callback(bot, ctx['callback_id'])
                with self.changed:
                    self.changed.notify_all()
            except Exception as exc:
                self.store.set_bot(bot, error='receiver_internal_error')
                log.error('bot=%s receiver_error=%s', bot, type(exc).__name__)
                self.stop.wait(5)

    def ack_callback(self, bot, callback_id):
        status, result = self.upstream.api(bot, 'answerCallbackQuery', {'callback_query_id': callback_id}, timeout=5)
        if result.get('ok'):
            with self.store.db() as db:
                db.execute('UPDATE callbacks SET acked=1 WHERE bot=? AND id=?', (bot, callback_id))

    def poll(self, binding, params):
        if self.cfg['apps'][binding['app']]['mode'] != 'poll':
            return failure(403, 'application_does_not_poll')
        try:
            offset, limit = int(params.get('offset', 0)), int(params.get('limit', 100))
            timeout = min(30, max(0, int(params.get('timeout', 0))))
            if not -2**63 < offset < 2**63 or not 1 <= limit <= 100:
                raise ValueError()
            limit = min(limit, int(binding.get('poll_limit', 100)))
        except (ValueError, TypeError):
            return failure(400, 'invalid_poll_parameters')
        lock = self.poll_locks[binding['name']]
        if not lock.acquire(blocking=False):
            return failure(409, 'another_application_poll_is_active')
        try:
            deadline = time.monotonic() + timeout
            while not self.stop.is_set():
                # Hold the notification condition across query/wait to avoid a missed wakeup.
                with self.changed:
                    updates = self.store.poll(binding['name'], offset, limit,
                                              ack_on_delivery=binding.get('ack_on_delivery', False))
                    if updates or time.monotonic() >= deadline:
                        return 200, {'ok': True, 'result': updates}
                    self.changed.wait(min(1, max(0, deadline-time.monotonic())))
            return 200, {'ok': True, 'result': []}
        finally:
            lock.release()

    def deliver(self, name):
        binding = self.cfg['bindings'][name]
        app = self.cfg['apps'][binding['app']]
        while not self.stop.is_set():
            if not self.store.bot(binding['bot'])['active']:
                self.stop.wait(.5)
                continue
            row = self.store.due(name)
            if not row:
                self.stop.wait(.5)
                continue
            try:
                response = self.upstream.session(external=False).post(app['endpoint'], data=row['body'].encode(),
                    headers={'Content-Type': 'application/json', app['secret_header']: app['webhook_secret'],
                             'X-Telegram-Bot-Id': str(self.cfg['bots'][binding['bot']]['id']),
                             'X-Telegram-Bot-Username': json.loads(self.store.bot(binding['bot'])['identity'] or '{}').get('username', '')},
                    timeout=(3, 15), allow_redirects=False)
                with response:
                    accepted = 200 <= response.status_code < 300
                    permanent = response.status_code in (400, 404, 410, 422)
                    error = None if accepted else 'webhook_http_' + str(response.status_code)
                self.store.delivery(row, accepted=accepted, error=error, permanent=permanent,
                                    delay=min(300, 2 ** min(row['attempts']+1, 8)))
            except requests.RequestException:
                self.store.delivery(row, error='webhook_unavailable', delay=min(300, 2 ** min(row['attempts']+1, 8)))
            except Exception as exc:
                log.error('binding=%s delivery_error=%s', name, type(exc).__name__)
                self.stop.wait(3)

    def authorized(self, binding, method, params):
        capabilities = binding['capabilities']
        if method in READ:
            if method == 'getFile' and 'files' not in capabilities:
                return False
            if method == 'getFile' and not self.store.file_allowed(binding, file_id=params.get('file_id')):
                return False
        elif method in TOPICS:
            if 'topics' not in capabilities:
                return False
        elif method not in SEND or 'send' not in capabilities:
            return False
        if 'chat_id' in params:
            if str(params['chat_id']) not in binding['allowed_chats']:
                return False
            if binding['topics'] and not any(str(t['chat_id']) == str(params['chat_id']) and
                    int(t['thread_id']) == int(params.get('message_thread_id', 0)) for t in binding['topics']):
                # Edits identify the topic through their owned message, not message_thread_id.
                if not method.startswith('edit') and method != 'deleteMessage':
                    return False
        if 'from_chat_id' in params and str(params['from_chat_id']) not in binding['allowed_chats']:
            return False
        if params.get('message_id') and params.get('chat_id'):
            owner = self.store.owner(binding['bot'], params['chat_id'], int(params['message_id']))
            if owner and owner != binding['name']:
                return False
        if method == 'answerCallbackQuery':
            with self.store.db() as db:
                row = db.execute('SELECT binding FROM callbacks WHERE bot=? AND id=?',
                                 (binding['bot'], params.get('callback_query_id'))).fetchone()
            if not row or row['binding'] != binding['name']:
                return False
        return True

    def api(self, binding, method, params, *, body=None, content_type=None, request_id=None):
        params = dict(params)
        bot = binding['bot']
        if method == 'getUpdates':
            return self.poll(binding, params)
        if method in ('getMyCommands', 'setMyCommands', 'deleteMyCommands'):
            return self.commands(binding, method, params)
        if method == 'getWebhookInfo':
            return 200, {'ok': True, 'result': {'url': '', 'has_custom_certificate': False, 'pending_update_count': 0}}
        if method == 'deleteWebhook':
            # Native clients may clear their local receiver configuration, never the real bot webhook.
            return 200, {'ok': True, 'result': True}
        if method == 'getMe':
            identity = self.store.bot(bot)['identity']
            if not identity and not self.identity(bot):
                return failure(503, 'bot_identity_unavailable')
            return 200, {'ok': True, 'result': json.loads(self.store.bot(bot)['identity'])}
        extra = {}
        if method.startswith(('send', 'forward', 'copy')) and 'message_thread_id' not in params:
            try:
                thread = self.store.reply_thread(binding, params)
            except ValueError:
                return failure(400, 'message_thread_id_required')
            if thread:
                params['message_thread_id'] = thread
                extra['message_thread_id'] = thread
        if not self.authorized(binding, method, params):
            return failure(403, 'application_permission_denied')
        if not self.store.bot(bot)['active']:
            return failure(503, 'bot_not_active')
        if method == 'answerCallbackQuery' and not params.get('text'):
            with self.store.db() as db:
                row = db.execute('SELECT acked FROM callbacks WHERE bot=? AND id=?', (bot, params['callback_query_id'])).fetchone()
            if row and row['acked']:
                return 200, {'ok': True, 'result': True}
        receipt = binding['name'] + ':' + request_id if request_id else uuid.uuid4().hex
        digest = hashlib.sha256((method + json.dumps(params, sort_keys=True, ensure_ascii=False)).encode())
        if request_id and body is not None:
            body.seek(0)
            while chunk := body.read(65536):
                digest.update(chunk)
        fingerprint = digest.hexdigest()
        with self.store.db(immediate=True) as db:
            old = db.execute('SELECT * FROM outbound WHERE id=?', (receipt,)).fetchone()
            if old:
                if old['fingerprint'] != fingerprint:
                    return failure(409, 'request_id_conflict')
                if old['response']:
                    cached = json.loads(old['response'])
                    code = int(cached.get('error_code', 200))
                    if cached.get('ok') or code not in (429, 500, 502, 503, 504):
                        return 200 if cached.get('ok') else code, cached
                    # A definite rejection is safe to retry with the same application request ID.
                    db.execute("UPDATE outbound SET status='sending',finished=NULL,error=NULL,response=NULL WHERE id=?", (receipt,))
                else:
                    return failure(409, 'outbound_outcome_unknown')
            else:
                db.execute('INSERT INTO outbound(id,binding,method,status,created,chat,thread,fingerprint) VALUES(?,?,?,?,?,?,?,?)',
                           (receipt, binding['name'], method, 'sending', time.time(), str(params.get('chat_id', '')),
                            int(params.get('message_thread_id', 0)), fingerprint))
        state = self.store.bot(bot)
        if state['pause_until'] > time.time():
            status, result = 429, {'ok': False, 'error_code': 429, 'description': 'bot_rate_limited',
                                  'parameters': {'retry_after': max(1, int(state['pause_until']-time.time())+1)}}
        else:
            status, result = self.upstream.api(bot, method, params, body=body, content_type=content_type, extra=extra)
        if result.get('ok'):
            self.store.remember(bot, binding['name'], result.get('result'))
            if method == 'getFile' and result.get('result', {}).get('file_path'):
                with self.store.db() as db:
                    db.execute('UPDATE files SET path=? WHERE bot=? AND binding=? AND id=?',
                        (result['result']['file_path'], bot, binding['name'], params['file_id']))
        elif result.get('error_code') == 429:
            self.store.set_bot(bot, pause_until=time.time()+int(result.get('parameters', {}).get('retry_after', 1)))
        with self.store.db() as db:
            message = result.get('result')
            if isinstance(message, list):
                message = message[0] if message else None
            message_id = message.get('message_id') if isinstance(message, dict) else None
            db.execute('UPDATE outbound SET status=?,finished=?,error=?,response=?,message_id=? WHERE id=?',
                ('sent' if result.get('ok') else 'unknown' if result.get('gateway_uncertain') else 'failed', time.time(),
                 None if result.get('ok') else str(result.get('description', 'api_error'))[:300],
                 json.dumps(result) if not result.get('gateway_uncertain') else None, message_id, receipt))
        return status, result

    def commands(self, binding, method, params):
        if 'commands' not in binding['capabilities']:
            return failure(403, 'application_permission_denied')
        try:
            scope = params.get('scope', {'type': 'default'})
            scope = json.loads(scope) if isinstance(scope, str) else scope
            if not isinstance(scope, dict) or 'type' not in scope:
                raise ValueError()
            if scope.get('chat_id') is not None and str(scope['chat_id']) not in binding['allowed_chats']:
                return failure(403, 'application_permission_denied')
            scope_key = json.dumps(scope, sort_keys=True, separators=(',', ':'))
            language = params.get('language_code', '')
            if not isinstance(language, str) or language and not re.fullmatch(r'[a-z]{2,3}', language):
                raise ValueError()
            if method == 'getMyCommands':
                with self.store.db() as db:
                    row = db.execute('SELECT body FROM commands WHERE binding=? AND scope=? AND language=?',
                                     (binding['name'], scope_key, language)).fetchone()
                return 200, {'ok': True, 'result': json.loads(row[0]) if row else binding['menu']}
            commands = params.get('commands', []) if method == 'setMyCommands' else []
            commands = json.loads(commands) if isinstance(commands, str) else commands
            if not isinstance(commands, list) or len(commands) > 100 or any(
                    not isinstance(c, dict) or not re.fullmatch(r'[a-z0-9_]{1,32}', c.get('command', '')) or
                    not isinstance(c.get('description'), str) or not 1 <= len(c['description']) <= 256 for c in commands):
                raise ValueError()
            with self.store.db() as db:
                db.execute('INSERT OR REPLACE INTO commands VALUES(?,?,?,?)',
                           (binding['name'], scope_key, language, json.dumps(commands, ensure_ascii=False)))
            with self.command_lock:
                self.menu_dirty[binding['bot']] += 1
            return 200, {'ok': True, 'result': True}
        except (ValueError, TypeError):
            return failure(400, 'invalid_commands')

    def menus(self):
        while not self.stop.wait(1):
            for bot in self.cfg['bots']:
                state = self.store.bot(bot)
                if not self.menu_dirty[bot] or not state['active'] or state['pause_until'] > time.time():
                    continue
                try:
                    with self.command_lock:
                        revision = self.menu_dirty[bot]
                    scopes = {('{"type":"default"}', '')}
                    bindings = [b for b in self.cfg['bindings'].values() if b['bot'] == bot]
                    with self.store.db() as db:
                        rows = {b['name']: db.execute('SELECT * FROM commands WHERE binding=?', (b['name'],)).fetchall() for b in bindings}
                    scopes.update((r['scope'], r['language']) for values in rows.values() for r in values)
                    okay = True
                    claimed = {command: b['name'] for b in bindings for command in b['commands']}
                    for scope, language in scopes:
                        merged = {}
                        for b in bindings:
                            selected = [r for r in rows[b['name']] if (r['scope'], r['language']) == (scope, language)]
                            commands = json.loads(selected[0]['body']) if selected else b['menu'] if scope == '{"type":"default"}' and not language else []
                            for command in commands:
                                if claimed.get(command['command'], b['name']) == b['name']:
                                    merged.setdefault(command['command'], command)
                        if len(merged) > 100:
                            raise ValueError('too_many_commands')
                        status, result = self.upstream.api(bot, 'setMyCommands', {'commands': list(merged.values()),
                            'scope': json.loads(scope), 'language_code': language}, timeout=10)
                        if result.get('error_code') == 429:
                            self.store.set_bot(bot, pause_until=time.time()+int(result.get('parameters', {}).get('retry_after', 1)))
                        okay = okay and result.get('ok', False)
                    with self.command_lock:
                        if okay and self.menu_dirty[bot] == revision:
                            self.menu_dirty[bot] = 0
                except Exception as exc:
                    log.error('bot=%s menu_error=%s', bot, type(exc).__name__)
                    self.stop.wait(5)
