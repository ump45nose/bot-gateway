"""Public command namespaces without changing an application's native commands."""
import copy
import re


def public_name(binding, name):
    prefix = binding.get('command_prefix', '')
    return prefix + '_' + name if prefix else name


def application_update(binding, update):
    prefix = binding.get('command_prefix', '')
    message = update.get('message', {})
    text = message.get('text', '')
    match = re.match(r'^/' + re.escape(prefix) + r'_([a-z0-9_]+)(?=@|\s|$)', text) if prefix else None
    if not match:
        return update
    result = copy.deepcopy(update)
    message = result['message']
    removed = len(prefix) + 1
    message['text'] = '/' + text[removed+1:]
    for entity in message.get('entities', []):
        if entity.get('type') == 'bot_command' and entity.get('offset') == 0:
            entity['length'] -= removed
        elif entity.get('offset', 0) >= match.end():
            entity['offset'] -= removed
    return result
