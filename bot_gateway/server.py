from email.message import Message
import hmac
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import re
import tempfile
from urllib.parse import parse_qs, unquote, urlsplit

import requests

from .engine import failure


def fields(body, content_type):
    """Read control fields, leaving binary file parts on disk for upstream streaming."""
    body.seek(0)
    header = Message()
    header['Content-Type'] = content_type
    kind = header.get_content_type()
    if kind == 'multipart/form-data':
        boundary = header.get_boundary()
        if not boundary or len(boundary) > 200:
            raise ValueError()
        marker = b'--' + boundary.encode()
        result, in_headers, headers, value, name = {}, False, [], [], None
        while line := body.readline(65536):
            if line.rstrip(b'\r\n') in (marker, marker+b'--'):
                if name is not None:
                    result[name] = b''.join(value).rstrip(b'\r\n').decode()
                in_headers, headers, value, name = True, [], [], None
            elif in_headers:
                if line in (b'\r\n', b'\n'):
                    part = Message()
                    for raw in headers:
                        key, item = raw.decode().split(':', 1)
                        part[key] = item.strip()
                    if part.get_filename() is None:
                        name = part.get_param('name', header='content-disposition')
                    in_headers = False
                else:
                    headers.append(line)
                    if sum(map(len, headers)) > 8192:
                        raise ValueError()
            elif name is not None:
                value.append(line)
                if sum(map(len, value)) > 1024**2:
                    raise ValueError()
        return result
    raw = body.read(1024**2+1)
    if len(raw) > 1024**2:
        raise ValueError()
    if not raw:
        return {}
    if kind == 'application/json':
        value = json.loads(raw)
        if not isinstance(value, dict):
            raise ValueError()
        return value
    return {key: values[-1] for key, values in parse_qs(raw.decode(), keep_blank_values=True).items()}


def server(gateway):
    class Handler(BaseHTTPRequestHandler):
        server_version = 'BotGateway/1'
        protocol_version = 'HTTP/1.1'

        def log_message(self, *_):
            # Native Bot API paths contain application credentials.
            pass

        def json(self, status, value):
            # ShellCrash's existing consumer parses Telegram's compact JSON with shell patterns.
            raw = json.dumps(value, ensure_ascii=False, separators=(',', ':')).encode()
            self.send_response(status)
            self.send_header('Content-Type', 'application/json; charset=utf-8')
            self.send_header('Cache-Control', 'no-store')
            if status >= 400:
                self.send_header('Connection', 'close')
                self.close_connection = True
            self.send_header('Content-Length', str(len(raw)))
            self.end_headers()
            self.wfile.write(raw)

        def do_GET(self):
            self.handle_api()

        def do_POST(self):
            self.handle_api()

        def handle_api(self):
            path = urlsplit(self.path)
            if path.path == '/health' and self.command == 'GET':
                self.json(200, {'ok': True})
                return
            admin = path.path.startswith('/v1/admin/')
            if admin:
                credential = self.headers.get('Authorization', '').removeprefix('Bearer ')
                if not hmac.compare_digest(credential.encode(), gateway.cfg['admin_secret'].encode()):
                    self.json(*failure(401, 'unauthorized'))
                    return
            match = re.fullmatch(r'/(file/)?bot([^/]+)/(.+)', path.path)
            binding = gateway.authenticate(unquote(match[2])) if match else None
            if not admin and binding is None:
                self.json(*failure(401, 'unauthorized'))
                return
            try:
                length = int(self.headers.get('Content-Length', '0'))
                if not 0 <= length <= gateway.cfg['max_request_bytes'] or self.headers.get('Transfer-Encoding'):
                    self.json(*failure(413, 'request_too_large_or_chunked'))
                    return
                self.connection.settimeout(40)
                with tempfile.TemporaryFile(dir=gateway.store.root) as body:
                    remaining = length
                    while remaining:
                        chunk = self.rfile.read(min(65536, remaining))
                        if not chunk:
                            raise ValueError()
                        body.write(chunk)
                        remaining -= len(chunk)
                    params = {key: values[-1] for key, values in parse_qs(path.query, keep_blank_values=True).items()}
                    content_type = self.headers.get('Content-Type', 'application/x-www-form-urlencoded')
                    params.update(fields(body, content_type))
                    if admin:
                        if path.path == '/v1/admin/status' and self.command == 'GET':
                            self.json(200, {'ok': True, 'result': gateway.store.status()})
                        elif re.fullmatch(r'/v1/admin/bots/[a-z][a-z0-9_-]*/active', path.path) and self.command == 'POST':
                            self.json(*gateway.activate(path.path.split('/')[4], params.get('active')))
                        else:
                            self.json(*failure(404, 'not_found'))
                    elif match[1]:
                        self.file(binding, unquote(match[3]))
                    else:
                        request_id = self.headers.get('X-Request-Id')
                        if request_id and (len(request_id) > 128 or not request_id.isprintable()):
                            raise ValueError()
                        self.json(*gateway.api(binding, match[3], params,
                            body=body if length and self.command == 'POST' else None,
                            content_type=content_type, request_id=request_id))
            except (ValueError, TypeError, KeyError, UnicodeError):
                self.json(*failure(400, 'invalid_request'))
            except (BrokenPipeError, ConnectionResetError, TimeoutError):
                pass
            except Exception:
                self.json(*failure(500, 'gateway_internal_error'))

        def file(self, binding, relative):
            if ('files' not in binding['capabilities'] or '..' in relative.split('/') or
                    not gateway.store.file_allowed(binding, path=relative)):
                self.json(*failure(403, 'file_permission_denied'))
                return
            try:
                with gateway.upstream.session().get(gateway.upstream.url(binding['bot'], relative, file=True),
                        stream=True, timeout=(5, 35), allow_redirects=False, headers={'Accept-Encoding': 'identity'}) as response:
                    self.send_response(response.status_code)
                    self.send_header('Content-Type', response.headers.get('Content-Type', 'application/octet-stream'))
                    self.send_header('Cache-Control', 'no-store')
                    self.send_header('Connection', 'close')
                    self.close_connection = True
                    self.end_headers()
                    for chunk in response.iter_content(65536):
                        self.wfile.write(chunk)
            except requests.RequestException:
                self.close_connection = True

    result = ThreadingHTTPServer((gateway.cfg['host'], gateway.cfg['port']), Handler)
    result.daemon_threads = True
    return result
