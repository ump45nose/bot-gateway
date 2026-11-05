import threading
from email.message import Message
import json

import requests


class Upstream:
    def __init__(self, cfg):
        self.cfg = cfg
        self.local = threading.local()

    def session(self, *, external=True):
        key = 'external' if external else 'internal'
        session = getattr(self.local, key, None)
        if session is None:
            session = requests.Session()
            session.trust_env = False
            if external and self.cfg['proxy']:
                session.proxies.update({'http': self.cfg['proxy'], 'https': self.cfg['proxy']})
            setattr(self.local, key, session)
        return session

    def url(self, bot, method, *, file=False):
        prefix = '/file/bot' if file else '/bot'
        return self.cfg['upstream_base'].rstrip('/') + prefix + self.cfg['bots'][bot]['token'] + '/' + method

    def api(self, bot, method, params=None, *, body=None, content_type=None, timeout=35, extra=None):
        try:
            kwargs = {'timeout': (5, timeout), 'allow_redirects': False, 'stream': True}
            if body is None:
                kwargs['json'] = params or {}
            else:
                body.seek(0)
                if extra and not content_type.startswith('multipart/'):
                    kwargs['json'] = params
                else:
                    if extra:
                        header = Message()
                        header['Content-Type'] = content_type
                        boundary = header.get_boundary().encode()
                        ending = b'--' + boundary + b'--'
                        body.seek(0, 2)
                        size = body.tell()
                        body.seek(max(0, size-len(ending)-8))
                        tail = body.read()
                        position = tail.rfind(ending)
                        if position < 0:
                            raise ValueError()
                        body.seek(max(0, size-len(ending)-8)+position)
                        body.truncate()
                        for key, value in extra.items():
                            body.write(b'--'+boundary+b'\r\nContent-Disposition: form-data; name="'+key.encode()+b'"\r\n\r\n'+str(value).encode()+b'\r\n')
                        body.write(ending+b'\r\n')
                    body.seek(0)
                    kwargs.update(data=body, headers={'Content-Type': content_type})
            with self.session().post(self.url(bot, method), **kwargs) as response:
                # Bot API metadata responses are small; file responses use the streaming path.
                data = bytearray()
                for chunk in response.iter_content(65536):
                    data.extend(chunk)
                    if len(data) > 8 * 1024**2:
                        raise ValueError()
                value = json.loads(data)
                if not isinstance(value, dict) or 'ok' not in value:
                    raise ValueError()
                return response.status_code, value
        except (requests.RequestException, ValueError, OSError):
            return 502, {'ok': False, 'error_code': 502, 'description': 'upstream_request_failed', 'gateway_uncertain': True}
