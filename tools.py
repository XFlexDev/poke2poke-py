import asyncio
import ipaddress
import json
import logging
import mimetypes
import os
import re
import secrets
import socket
import urllib.error
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from fastmcp import Context
from fastmcp.server.dependencies import get_http_request
from config import TOKENS, CDN_UPLOAD_URL, CDN_HOSTS, MAX_MESSAGE_CHARS, MAX_ATTACHMENTS, MAX_UPLOAD_BYTES
from db import connection
from poke_client import push_to_poke, spawn

log = logging.getLogger('poke2poke.tools')
MAX_RESULTS = 100
MAX_REDIRECTS = 5
_UNSAFE_FILENAME = re.compile(r'[\x00-\x1f\x7f"\\/]')


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


async def caller(ctx: Context | None = None) -> str:
    try:
        auth = get_http_request().headers.get('authorization', '')
    except Exception:
        auth = ''
    if not auth.startswith('Bearer '):
        raise ValueError('unauthorized: Authorization: Bearer <configured key> required')
    token = auth[7:].strip()
    matched = None
    for who, key in TOKENS.items():
        if secrets.compare_digest(token.encode(), key.encode()):
            matched = who
    if matched is None:
        raise ValueError('unauthorized: invalid bearer token')
    return matched


def _cdn_url(url: str) -> bool:
    p = urllib.parse.urlparse(url)
    return p.scheme == 'https' and (p.hostname or '').lower() in CDN_HOSTS


def _public_host(hostname: str) -> None:
    """Reject hostnames that resolve to loopback, private, link-local or otherwise non-public addresses (SSRF guard)."""
    try:
        infos = socket.getaddrinfo(hostname, 443, proto=socket.IPPROTO_TCP)
    except socket.gaierror as e:
        raise ValueError(f'cannot resolve host {hostname!r}') from e
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        if not ip.is_global or ip.is_multicast:
            raise ValueError(f'host {hostname!r} resolves to a non-public address')


def _check_fetch_url(url: str) -> str:
    p = urllib.parse.urlparse(url)
    if p.scheme != 'https' or not p.hostname:
        raise ValueError('source_url must be HTTPS')
    _public_host(p.hostname)
    return url


class _NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


_opener = urllib.request.build_opener(_NoRedirect)


def _open_checked(url: str, timeout: float):
    """Open url, following at most MAX_REDIRECTS redirects, re-validating every hop against the SSRF guard."""
    for _ in range(MAX_REDIRECTS + 1):
        req = urllib.request.Request(_check_fetch_url(url), headers={'User-Agent': 'poke2poke/1.0'})
        try:
            return _opener.open(req, timeout=timeout)
        except urllib.error.HTTPError as e:
            if e.code in (301, 302, 303, 307, 308) and e.headers.get('Location'):
                url = urllib.parse.urljoin(url, e.headers['Location'])
                continue
            raise ValueError(f'fetching source_url failed with HTTP {e.code}') from e
    raise ValueError('too many redirects')


def _safe_filename(name: str) -> str:
    name = _UNSAFE_FILENAME.sub('_', os.path.basename(name or '')).strip(' .')
    return name[:255] or 'attachment'


def register(mcp):
    @mcp.tool()
    async def send_poke_message(recipient: str, message: str = '', attachment_urls: list[str] | None = None, ctx: Context | None = None) -> str:
        """Send a message (and/or CDN1 attachment URLs) to another configured Poke identity. Returns the message id."""
        sender = await caller(ctx)
        recipient = (recipient or '').strip()
        message = message or ''
        urls = [u.strip() for u in (attachment_urls or []) if u and u.strip()]
        if recipient not in TOKENS:
            raise ValueError('unknown recipient')
        if not message.strip() and not urls:
            raise ValueError('message or attachment_urls is required')
        if len(message) > MAX_MESSAGE_CHARS:
            raise ValueError(f'message exceeds {MAX_MESSAGE_CHARS} characters')
        if len(urls) > MAX_ATTACHMENTS:
            raise ValueError(f'at most {MAX_ATTACHMENTS} attachments allowed')
        for url in urls:
            if not _cdn_url(url):
                raise ValueError('attachments must be HTTPS CDN1 URLs')
        mid = secrets.token_urlsafe(16)
        meta = json.dumps({'attachments': urls}, separators=(',', ':'))

        def insert():
            with connection() as c:
                c.execute('INSERT INTO messages (msg_id,sender_handle,recipient_handle,text,metadata,timestamp,status) VALUES (?,?,?,?,?,?,?)',
                          (mid, sender, recipient, message, meta, _now(), 'unread'))

        await asyncio.to_thread(insert)
        spawn(push_to_poke(recipient, message, sender, attachments=[{'url': u} for u in urls] or None), name=f'push:{mid}')
        return mid

    @mcp.tool()
    async def check_poke_messages(unread_only: bool = True, limit: int = MAX_RESULTS, ctx: Context | None = None) -> list[dict]:
        """List messages addressed to the caller, oldest first."""
        who = await caller(ctx)
        limit = max(1, min(int(limit), MAX_RESULTS))
        q = 'SELECT msg_id,sender_handle,recipient_handle,text,metadata,timestamp,status,acknowledged_at FROM messages WHERE recipient_handle=?'
        args = [who]
        if unread_only:
            q += " AND status != 'read'"
        q += ' ORDER BY timestamp ASC, msg_id ASC LIMIT ?'
        args.append(limit)

        def query():
            with connection() as c:
                return [dict(r) for r in c.execute(q, args)]

        out = await asyncio.to_thread(query)
        for d in out:
            try:
                meta = json.loads(d['metadata'] or '{}')
                d['metadata'] = meta if isinstance(meta, dict) else {}
            except (json.JSONDecodeError, TypeError):
                d['metadata'] = {}
        return out

    @mcp.tool()
    async def ack_poke_message(message_id: str, note: str = '', ctx: Context | None = None) -> str:
        """Mark a received message as read and notify its sender. Idempotent: a second ack returns 'already acknowledged'."""
        who = await caller(ctx)
        message_id = (message_id or '').strip()
        note = (note or '').strip()
        if not message_id:
            raise ValueError('message_id is required')
        if len(note) > MAX_MESSAGE_CHARS:
            raise ValueError(f'note exceeds {MAX_MESSAGE_CHARS} characters')

        def ack():
            with connection() as c:
                row = c.execute('SELECT sender_handle,status FROM messages WHERE msg_id=? AND recipient_handle=?', (message_id, who)).fetchone()
                if not row:
                    return None, None
                if row['status'] == 'read':
                    return row['sender_handle'], False
                c.execute("UPDATE messages SET status='read', acknowledged_at=? WHERE msg_id=? AND recipient_handle=? AND status != 'read'",
                          (_now(), message_id, who))
                return row['sender_handle'], c.execute('SELECT changes()').fetchone()[0] == 1

        sender, changed = await asyncio.to_thread(ack)
        if sender is None:
            return 'not found'
        if not changed:
            return 'already acknowledged'
        text = f'{who} acknowledged your message' + (f': {note}' if note else '')
        spawn(push_to_poke(sender, text, who, event='acknowledgment', note=note, status='acknowledged'), name=f'ack:{message_id}')
        return 'ok'

    @mcp.tool()
    async def upload_poke_file(source_url: str, filename: str = 'attachment', ctx: Context | None = None) -> dict:
        """Fetch a public HTTPS file in memory and upload it to CDN1. Returns the CDN1 URL."""
        await caller(ctx)
        source_url = (source_url or '').strip()
        _check_fetch_url(source_url)
        safe_name = _safe_filename(filename)

        def upload():
            with _open_checked(source_url, timeout=60) as r:
                declared = r.headers.get('Content-Length')
                if declared and declared.isdigit() and int(declared) > MAX_UPLOAD_BYTES:
                    raise ValueError(f'file exceeds {MAX_UPLOAD_BYTES} byte limit')
                data = r.read(MAX_UPLOAD_BYTES + 1)
                ctype = r.headers.get_content_type()
            if len(data) > MAX_UPLOAD_BYTES:
                raise ValueError(f'file exceeds {MAX_UPLOAD_BYTES} byte limit')
            if not data:
                raise ValueError('source_url returned an empty body')
            if not ctype or ctype == 'application/octet-stream':
                ctype = mimetypes.guess_type(safe_name)[0] or 'application/octet-stream'
            boundary = '----poke2poke-' + secrets.token_hex(16)
            head = (f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{safe_name}"\r\n'
                    f'Content-Type: {ctype}\r\n\r\n').encode()
            body = head + data + f'\r\n--{boundary}--\r\n'.encode()
            req = urllib.request.Request(CDN_UPLOAD_URL, data=body,
                                         headers={'Content-Type': f'multipart/form-data; boundary={boundary}'}, method='POST')
            try:
                with urllib.request.urlopen(req, timeout=120) as up_r:
                    raw = up_r.read(1024 * 1024)
            except urllib.error.HTTPError as e:
                raise ValueError(f'CDN1 upload failed with HTTP {e.code}') from e
            try:
                result = json.loads(raw.decode('utf-8', 'replace'))
            except json.JSONDecodeError as e:
                raise ValueError('CDN1 upload returned non-JSON response') from e
            if not isinstance(result, dict):
                raise ValueError('CDN1 upload returned unexpected response')
            url = result.get('url') or result.get('cdn_url') or result.get('fileUrl')
            if not isinstance(url, str) or not _cdn_url(url):
                raise ValueError('CDN1 upload did not return a valid CDN1 URL')
            return {'url': url, 'filename': safe_name, 'content_type': ctype, 'size': len(data)}

        return await asyncio.to_thread(upload)

    return mcp
