import asyncio
import json
import logging
import urllib.error
import urllib.request
from config import API_KEYS, POKE_INBOUND_URL

log = logging.getLogger('poke2poke.client')
_background: set[asyncio.Task] = set()
RETRIES = 3


def spawn(coro, name: str) -> asyncio.Task:
    """Run coro in the background, keeping a strong reference and logging any failure."""
    task = asyncio.get_running_loop().create_task(coro, name=name)
    _background.add(task)

    def _done(t: asyncio.Task):
        _background.discard(t)
        if not t.cancelled() and t.exception() is not None:
            log.error('background task %s failed', t.get_name(), exc_info=t.exception())

    task.add_done_callback(_done)
    return task


async def push_to_poke(recipient, message, sender, *, event='message', note='', status='', attachments=None) -> bool:
    key = API_KEYS.get(recipient)
    if not key:
        log.warning('no Poke API key configured for recipient=%s; skipping inbound push', recipient)
        return False
    obj = {'message': message, 'text': message, 'sender': sender, 'actor': sender, 'event': event, 'type': event,
           'status': status or ('acknowledged' if event == 'acknowledgment' else 'sent')}
    if note:
        obj['note'] = note
    if attachments:
        obj['attachments'] = attachments
    payload = json.dumps(obj, ensure_ascii=False).encode()

    def post():
        req = urllib.request.Request(POKE_INBOUND_URL, data=payload,
                                     headers={'Authorization': 'Bearer ' + key, 'Content-Type': 'application/json'}, method='POST')
        try:
            with urllib.request.urlopen(req, timeout=20) as r:
                return r.status, r.read(4096).decode('utf-8', 'replace')
        except urllib.error.HTTPError as e:
            return e.code, e.read(4096).decode('utf-8', 'replace')

    for attempt in range(1, RETRIES + 1):
        try:
            code, body = await asyncio.to_thread(post)
            if code < 400:
                log.info('poke inbound recipient=%s event=%s status=%s', recipient, event, code)
                return True
            log.warning('poke inbound recipient=%s event=%s attempt=%d status=%s response=%s', recipient, event, attempt, code, body[:500])
            if code < 500:
                return False
        except Exception as e:
            log.warning('poke inbound recipient=%s event=%s attempt=%d error=%r', recipient, event, attempt, e)
        if attempt < RETRIES:
            await asyncio.sleep(0.5 * attempt)
    log.error('poke inbound recipient=%s event=%s gave up after %d attempts', recipient, event, RETRIES)
    return False
