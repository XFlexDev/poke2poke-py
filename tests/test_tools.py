import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

_tmp = tempfile.mkdtemp()
os.environ.update({
    'POKE2POKE_CONFIG': os.path.join(_tmp, 'missing.json'),
    'POKE2POKE_DATA': os.path.join(_tmp, 'test.sqlite'),
    'POKE_INBOUND_URL': 'https://example.invalid/inbound',
    'CDN1_UPLOAD_URL': 'https://example.invalid/upload',
    'CDN1_HOSTS': 'cdn.example.com',
    'POKE2POKE_KEY_ALICE': 'alice-token',
    'POKE2POKE_KEY_BOB': 'bob-token',
    'POKE_API_KEY_ALICE': 'alice-api',
    'POKE_API_KEY_BOB': 'bob-api',
})

import tools  # noqa: E402
import poke_client  # noqa: E402
from fastmcp import FastMCP  # noqa: E402


class _Req:
    def __init__(self, token):
        self.headers = {'authorization': f'Bearer {token}'} if token else {}


@pytest.fixture
def api(monkeypatch):
    mcp = tools.register(FastMCP('t'))
    fns = {name: t.fn for name, t in asyncio.run(mcp.get_tools()).items()}
    pushes = []

    async def fake_push(recipient, message, sender, **kw):
        pushes.append((recipient, message, sender, kw))
        return True

    monkeypatch.setattr(tools, 'push_to_poke', fake_push)
    state = {'token': 'alice-token'}
    monkeypatch.setattr(tools, 'get_http_request', lambda: _Req(state['token']))

    class Api:
        def __init__(self):
            self.pushes = pushes

        def as_(self, token):
            state['token'] = token
            return self

        def call(self, name, **kw):
            async def run():
                r = await fns[name](**kw)
                await asyncio.gather(*poke_client._background)
                return r
            return asyncio.run(run())

    return Api()


def test_auth_required(api):
    with pytest.raises(ValueError, match='unauthorized'):
        api.as_('').call('check_poke_messages')
    with pytest.raises(ValueError, match='unauthorized'):
        api.as_('nope').call('check_poke_messages')


def test_send_check_ack_flow(api):
    mid = api.as_('alice-token').call('send_poke_message', recipient='bob', message='hi bob')
    assert api.pushes[-1][:3] == ('bob', 'hi bob', 'alice')

    msgs = api.as_('bob-token').call('check_poke_messages')
    assert [m['msg_id'] for m in msgs] == [mid]
    assert msgs[0]['status'] == 'unread' and msgs[0]['metadata'] == {'attachments': []}

    assert api.as_('alice-token').call('check_poke_messages') == []

    assert api.as_('bob-token').call('ack_poke_message', message_id=mid, note='thanks') == 'ok'
    assert api.pushes[-1][0] == 'alice' and api.pushes[-1][3]['event'] == 'acknowledgment'
    n = len(api.pushes)

    assert api.as_('bob-token').call('ack_poke_message', message_id=mid) == 'already acknowledged'
    assert len(api.pushes) == n, 'duplicate ack must not re-notify sender'
    assert api.as_('bob-token').call('check_poke_messages') == []
    assert api.as_('bob-token').call('check_poke_messages', unread_only=False)[0]['acknowledged_at']


def test_ack_only_by_recipient(api):
    mid = api.as_('alice-token').call('send_poke_message', recipient='bob', message='x')
    assert api.as_('alice-token').call('ack_poke_message', message_id=mid) == 'not found'
    assert api.as_('bob-token').call('ack_poke_message', message_id='nope') == 'not found'


def test_send_validation(api):
    a = api.as_('alice-token')
    with pytest.raises(ValueError, match='unknown recipient'):
        a.call('send_poke_message', recipient='carol', message='x')
    with pytest.raises(ValueError, match='required'):
        a.call('send_poke_message', recipient='bob', message='   ')
    with pytest.raises(ValueError, match='CDN1'):
        a.call('send_poke_message', recipient='bob', attachment_urls=['http://cdn.example.com/f'])
    with pytest.raises(ValueError, match='CDN1'):
        a.call('send_poke_message', recipient='bob', attachment_urls=['https://evil.example.com/f'])
    with pytest.raises(ValueError, match='exceeds'):
        a.call('send_poke_message', recipient='bob', message='x' * (tools.MAX_MESSAGE_CHARS + 1))
    with pytest.raises(ValueError, match='attachments'):
        a.call('send_poke_message', recipient='bob', attachment_urls=['https://cdn.example.com/f'] * (tools.MAX_ATTACHMENTS + 1))
    mid = a.call('send_poke_message', recipient='bob', attachment_urls=['https://CDN.example.com/f '])
    assert api.pushes[-1][3]['attachments'] == [{'url': 'https://CDN.example.com/f'}]
    assert mid


def test_check_limit(api):
    a = api.as_('alice-token')
    for i in range(3):
        a.call('send_poke_message', recipient='bob', message=str(i))
    assert len(api.as_('bob-token').call('check_poke_messages', limit=2)) == 2
    assert len(api.as_('bob-token').call('check_poke_messages', limit=0)) == 1
    assert len(api.as_('bob-token').call('check_poke_messages', limit=10 ** 6)) >= 3


def test_concurrent_acks_notify_once(api, monkeypatch):
    mid = api.as_('alice-token').call('send_poke_message', recipient='bob', message='race')
    api.as_('bob-token')
    mcp = tools.register(FastMCP('t2'))
    ack = asyncio.run(mcp.get_tools())['ack_poke_message'].fn

    async def run():
        results = await asyncio.gather(*[ack(message_id=mid) for _ in range(10)])
        await asyncio.gather(*poke_client._background)
        return results

    results = asyncio.run(run())
    assert sorted(results) == ['already acknowledged'] * 9 + ['ok']
    assert sum(1 for p in api.pushes if p[3].get('event') == 'acknowledgment') == 1


def test_safe_filename():
    assert tools._safe_filename('../../etc/passwd') == 'passwd'
    assert tools._safe_filename('a"b\r\nc.txt') == 'a_b__c.txt'
    assert tools._safe_filename('') == 'attachment'
    assert tools._safe_filename('..') == 'attachment'


def test_fetch_url_ssrf_guard():
    for bad in ['http://example.com/x', 'https://127.0.0.1/x', 'https://localhost/x', 'https://[::1]/x',
                'https://10.0.0.1/x', 'https://169.254.169.254/latest/meta-data', 'ftp://example.com/x', 'https:///x']:
        with pytest.raises(ValueError):
            tools._check_fetch_url(bad)
    assert tools._check_fetch_url('https://1.1.1.1/x') == 'https://1.1.1.1/x'


def test_push_to_poke_retries_on_5xx_and_gives_up(monkeypatch):
    calls = []

    class R:
        status = 500

        def __enter__(self): return self

        def __exit__(self, *a): pass

        def read(self, n=-1): return b'boom'

    monkeypatch.setattr(poke_client.urllib.request, 'urlopen', lambda req, timeout: calls.append(req) or R())
    real_sleep = asyncio.sleep
    monkeypatch.setattr(poke_client.asyncio, 'sleep', lambda s: real_sleep(0))
    assert asyncio.run(poke_client.push_to_poke('bob', 'm', 'alice')) is False
    assert len(calls) == poke_client.RETRIES
    assert json.loads(calls[0].data)['message'] == 'm'


def test_push_to_poke_no_retry_on_4xx(monkeypatch):
    calls = []

    class R:
        status = 401

        def __enter__(self): return self

        def __exit__(self, *a): pass

        def read(self, n=-1): return b''

    monkeypatch.setattr(poke_client.urllib.request, 'urlopen', lambda req, timeout: calls.append(1) or R())
    assert asyncio.run(poke_client.push_to_poke('bob', 'm', 'alice')) is False
    assert len(calls) == 1
    assert asyncio.run(poke_client.push_to_poke('nobody', 'm', 'alice')) is False
