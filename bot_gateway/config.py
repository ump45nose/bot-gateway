import hashlib
import re
import tomllib
from pathlib import Path
from urllib.parse import urlsplit

NAME = re.compile(r'[a-z][a-z0-9_-]{0,63}')
COMMAND = re.compile(r'[a-z0-9_]{1,32}')


def secret(path):
    value = Path(path).read_text().strip()
    if not value or any(c.isspace() for c in value):
        raise ValueError('invalid secret file')
    return value


def load(path):
    with open(path, 'rb') as stream:
        cfg = tomllib.load(stream)
    cfg.setdefault('host', '127.0.0.1')
    cfg.setdefault('port', 18198)
    cfg.setdefault('upstream_base', 'https://api.telegram.org')
    cfg.setdefault('proxy', '')
    cfg.setdefault('max_request_bytes', 64 * 1024**2)
    cfg['admin_secret'] = secret(cfg['admin_token_file'])
    cfg['bots'], cfg['apps'] = cfg.get('bots', {}), cfg.get('apps', {})
    if not cfg['bots'] or not cfg['apps']:
        raise ValueError('bots and apps are required')
    real_tokens, real_ids, credentials = set(), set(), set()
    for name, bot in cfg['bots'].items():
        if not NAME.fullmatch(name):
            raise ValueError('invalid bot name')
        bot['name'], bot['token'] = name, secret(bot['token_file'])
        if not re.fullmatch(r'[1-9][0-9]*:[A-Za-z0-9_-]+', bot['token']) or bot['token'] in real_tokens:
            raise ValueError('invalid or duplicate bot token')
        real_tokens.add(bot['token'])
        bot['id'] = int(bot['token'].split(':')[0])
        if bot['id'] in real_ids:
            raise ValueError('one receiver configuration per bot identity')
        real_ids.add(bot['id'])
        bot.setdefault('initially_active', False)
        bot.setdefault('allowed_users', [])
        if not bot['allowed_users'] or any(type(v) is not int or v <= 0 for v in bot['allowed_users']):
            raise ValueError('each bot needs allowed_users')
    for name, app in cfg['apps'].items():
        if not NAME.fullmatch(name) or app.get('mode') not in ('poll', 'webhook', 'send'):
            raise ValueError('invalid application')
        app['name'] = name
        if app['mode'] == 'webhook':
            endpoint = urlsplit(app['endpoint'])
            if endpoint.scheme not in ('http', 'https') or not endpoint.hostname or endpoint.username or endpoint.fragment:
                raise ValueError('invalid webhook endpoint')
            app['webhook_secret'] = secret(app['secret_file'])
            app.setdefault('secret_header', 'X-Telegram-Bot-Api-Secret-Token')
    bindings = {}
    for binding in cfg.get('bindings', []):
        name, bot, app = binding['name'], binding['bot'], binding['app']
        if not NAME.fullmatch(name) or name in bindings or bot not in cfg['bots'] or app not in cfg['apps']:
            raise ValueError('invalid binding')
        binding['credential'] = secret(binding['credential_file'])
        if (not re.fullmatch(str(cfg['bots'][bot]['id']) + r':[A-Za-z0-9_-]+', binding['credential']) or
                binding['credential'] in real_tokens or binding['credential'] in credentials):
            raise ValueError('binding needs a unique application credential')
        credentials.add(binding['credential'])
        binding['credential_hash'] = hashlib.sha256(binding['credential'].encode()).hexdigest()
        binding.setdefault('allowed_users', cfg['bots'][bot]['allowed_users'])
        binding.setdefault('allowed_chats', binding['allowed_users'])
        binding['allowed_chats'] = [str(value) for value in binding['allowed_chats']]
        if not set(binding['allowed_users']) <= set(cfg['bots'][bot]['allowed_users']):
            raise ValueError('binding users must be allowed by the bot')
        for field in ('commands', 'callback_values', 'callback_prefixes', 'topics', 'menu'):
            binding.setdefault(field, [])
        if any(not COMMAND.fullmatch(value) for value in binding['commands']):
            raise ValueError('invalid route command')
        binding.setdefault('default', False)
        binding.setdefault('telegram_links', False)
        binding.setdefault('date_ranges', False)
        binding.setdefault('capabilities', ['send', 'files', 'commands'])
        if not 1 <= binding.get('poll_limit', 100) <= 100 or binding.get('infer_thread', '') not in ('', 'serial', 'unique'):
            raise ValueError('invalid polling or reply context settings')
        if binding.get('infer_thread') == 'serial' and binding.get('poll_limit') != 1:
            raise ValueError('serial reply context requires poll_limit=1')
        if any(not COMMAND.fullmatch(item.get('command', '')) or not isinstance(item.get('description'), str)
               or not 1 <= len(item['description']) <= 256 for item in binding['menu']):
            raise ValueError('invalid static command menu')
        if not set(binding['capabilities']) <= {'send', 'files', 'commands', 'topics'}:
            raise ValueError('unknown binding capability')
        if app and cfg['apps'][app]['mode'] == 'send' and any(binding[k] for k in (
                'commands', 'callback_values', 'callback_prefixes', 'topics', 'default', 'telegram_links', 'date_ranges')):
            raise ValueError('send-only binding cannot receive updates')
        bindings[name] = binding
    for bot in cfg['bots']:
        selected = [b for b in bindings.values() if b['bot'] == bot]
        if not selected or sum(bool(b['default']) for b in selected) > 1:
            raise ValueError('bot needs bindings and at most one default route')
        commands, topics = set(), set()
        for binding in selected:
            if commands & set(binding['commands']):
                raise ValueError('command route collision')
            commands.update(binding['commands'])
            for topic in binding['topics']:
                identity = (str(topic['chat_id']), int(topic['thread_id']))
                if identity in topics:
                    raise ValueError('topic route collision')
                topics.add(identity)
    cfg['bindings'] = bindings
    return cfg
