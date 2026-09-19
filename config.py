import json
import os
from pathlib import Path

BASE = Path(__file__).resolve().parent
CONFIG_PATH = Path(os.environ.get('POKE2POKE_CONFIG', BASE / 'config.json'))
try:
    with open(CONFIG_PATH, encoding='utf-8') as f:
        CONFIG = json.load(f)
except FileNotFoundError:
    CONFIG = {}
except json.JSONDecodeError as e:
    raise SystemExit(f'invalid JSON in {CONFIG_PATH}: {e}') from e


def _setting(env_name, key, default=None):
    v = os.environ.get(env_name)
    if v is not None:
        return v
    v = CONFIG.get(key, default)
    if v is None:
        raise SystemExit(f'missing required setting {key!r} (or env {env_name})')
    return v


def _secrets(section, env_prefix):
    out = {k: (v or '').strip() for k, v in CONFIG.get(section, {}).items()}
    for name, v in os.environ.items():
        if name.startswith(env_prefix) and len(name) > len(env_prefix):
            out[name[len(env_prefix):].lower()] = v.strip()
    return {k: v for k, v in out.items() if v}


PORT = int(_setting('POKE2POKE_PORT', 'port', 8765))
DB_PATH = _setting('POKE2POKE_DATA', 'db_path', 'data.sqlite')
POKE_INBOUND_URL = _setting('POKE_INBOUND_URL', 'poke_inbound_url')
CDN_UPLOAD_URL = _setting('CDN1_UPLOAD_URL', 'cdn_upload_url')
_hosts = _setting('CDN1_HOSTS', 'cdn_hosts', [])
if isinstance(_hosts, str):
    _hosts = _hosts.split(',')
CDN_HOSTS = {h.strip().lower() for h in _hosts if h.strip()}
TOKENS = _secrets('tokens', 'POKE2POKE_KEY_')
API_KEYS = _secrets('api_keys', 'POKE_API_KEY_')
MAX_MESSAGE_CHARS = int(_setting('POKE2POKE_MAX_MESSAGE_CHARS', 'max_message_chars', 20000))
MAX_ATTACHMENTS = int(_setting('POKE2POKE_MAX_ATTACHMENTS', 'max_attachments', 10))
MAX_UPLOAD_BYTES = int(_setting('POKE2POKE_MAX_UPLOAD_BYTES', 'max_upload_bytes', 100 * 1024 * 1024))

if not TOKENS:
    raise SystemExit('no bearer tokens configured: set config "tokens" or POKE2POKE_KEY_<HANDLE> env vars')
