"""Add a configurable Bot API base to ShellCrash's existing receiver/sender.

Only the two known upstream URL assignments are changed. The proxy core,
remote-control actions, user checks and watchdog remain owned by ShellCrash.
"""
import argparse
from pathlib import Path


def patch(root):
    path = Path(root) / 'menus/bot_tg.sh'
    old = path.read_text()
    text = old
    replacements = {
        'API="https://api.telegram.org/bot$TG_TOKEN"': 'API="${TG_API_BASE:-https://api.telegram.org}/bot$TG_TOKEN"',
        'API_FILE="https://api.telegram.org/file/bot$TG_TOKEN"': 'API_FILE="${TG_API_BASE:-https://api.telegram.org}/file/bot$TG_TOKEN"',
    }
    for source, target in replacements.items():
        if target in text:
            continue
        if text.count(source) != 1:
            raise ValueError('ShellCrash receiver version changed; inspect before patching')
        text = text.replace(source, target, 1)
    if text != old:
        path.write_text(text)
    return text != old


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--root', default='/etc/ShellCrash')
    args = parser.parse_args()
    print('updated' if patch(args.root) else 'already configured')
