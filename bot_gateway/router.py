import re

LINK = re.compile(r'(?:@[A-Za-z][A-Za-z0-9_]{3,31}|(?:https?://)?(?:t\.me|telegram\.me)/[^\s]+)')
DATES = re.compile(r'\d{4}-\d{2}-\d{2}\s+\d{4}-\d{2}-\d{2}')


def context(update):
    callback = update.get('callback_query')
    message = callback.get('message', {}) if isinstance(callback, dict) else update.get('message', {})
    sender = callback.get('from', {}) if isinstance(callback, dict) else message.get('from', {})
    return {'user': sender.get('id'), 'chat': message.get('chat', {}).get('id'),
            'thread': message.get('message_thread_id', 0), 'message_id': message.get('message_id'),
            'reply_message_id': message.get('reply_to_message', {}).get('message_id'),
            'text': message.get('text', '').strip(), 'data': callback.get('data', '') if callback else '',
            'callback_id': callback.get('id') if callback else None}


def choose(cfg, store, bot, update):
    ctx = context(update)
    if ctx['user'] not in cfg['bots'][bot]['allowed_users'] or ctx['chat'] is None:
        return None, 'forbidden_user', ctx
    configured = [b for b in cfg['bindings'].values() if b['bot'] == bot and cfg['apps'][b['app']]['mode'] != 'send']
    candidates = [b for b in configured if ctx['user'] in b['allowed_users'] and
                  str(ctx['chat']) in b['allowed_chats']]
    if ctx['callback_id']:
        owner = store.owner(bot, ctx['chat'], ctx['message_id'])
        if owner:
            return (owner, 'message_owner', ctx) if any(b['name'] == owner for b in candidates) else (None, 'forbidden_binding', ctx)
        matches = [b for b in configured if ctx['data'] in b['callback_values'] or
                   any(ctx['data'].startswith(prefix) for prefix in b['callback_prefixes'])]
        if matches:
            return single([b for b in matches if b in candidates], 'callback_route', ctx)
    elif ctx['text'].startswith('/'):
        command = ctx['text'].split()[0].split('@', 1)
        identity = store.bot(bot).get('identity')
        if len(command) > 1 and identity:
            import json
            if command[1].casefold() != json.loads(identity).get('username', '').casefold():
                return None, 'other_bot_command', ctx
        matches = [b for b in configured if command[0][1:] in b['commands']]
        if matches:
            return single([b for b in matches if b in candidates], 'command_route', ctx)
    else:
        matches = [b for b in configured if b['telegram_links'] and LINK.fullmatch(ctx['text']) or
                   b['date_ranges'] and DATES.fullmatch(ctx['text'])]
        if matches:
            return single([b for b in matches if b in candidates], 'content_route', ctx)
    if ctx['reply_message_id']:
        owner = store.owner(bot, ctx['chat'], ctx['reply_message_id'])
        if owner and any(b['name'] == owner for b in candidates):
            return owner, 'reply_route', ctx
    owner = store.conversation(bot, ctx)
    if owner and any(b['name'] == owner for b in candidates):
        return owner, 'conversation_route', ctx
    matches = [b for b in candidates if any(str(t['chat_id']) == str(ctx['chat']) and
               int(t['thread_id']) == ctx['thread'] for t in b['topics'])]
    if matches:
        return single(matches, 'topic_route', ctx)
    return single([b for b in candidates if b['default']], 'default_route', ctx)


def single(bindings, reason, ctx):
    if len(bindings) != 1:
        return None, 'ambiguous_route' if bindings else 'no_route', ctx
    return bindings[0]['name'], reason, ctx
