import json, os
from pathlib import Path
BASE = Path(__file__).resolve().parent
with open(BASE / 'config.json', encoding='utf-8') as f:
    CONFIG = json.load(f)
def value(section, name, env_name):
    return os.environ.get(env_name, CONFIG.get(section, {}).get(name, '')) if section else os.environ.get(env_name, CONFIG.get(name, ''))
PORT = int(os.environ.get('POKE2POKE_PORT', CONFIG['port']))
DB_PATH = os.environ.get('POKE2POKE_DATA', CONFIG['db_path'])
POKE_INBOUND_URL = os.environ.get('POKE_INBOUND_URL', CONFIG['poke_inbound_url'])
CDN_UPLOAD_URL = os.environ.get('CDN1_UPLOAD_URL', CONFIG['cdn_upload_url'])
CDN_HOSTS = set(CONFIG['cdn_hosts'])
TOKENS = {k: os.environ.get('POKE2POKE_KEY_'+k.upper(), v).strip() for k,v in CONFIG['tokens'].items()}
API_KEYS = {k: os.environ.get('POKE_API_KEY_'+k.upper(), v).strip() for k,v in CONFIG['api_keys'].items()}
