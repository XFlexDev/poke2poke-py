import asyncio, json, urllib.request, urllib.error
from config import API_KEYS, POKE_INBOUND_URL
async def push_to_poke(recipient, message, sender, *, event='message', note='', status='', attachments=None):
    key=API_KEYS.get(recipient)
    if not key: return
    obj={'message':message,'text':message,'sender':sender,'actor':sender,'event':event,'type':event,'status':status or ('acknowledged' if event=='acknowledgment' else 'sent')}
    if note: obj['note']=note
    if attachments: obj['attachments']=attachments
    payload=json.dumps(obj,ensure_ascii=False).encode()
    def post():
        req=urllib.request.Request(POKE_INBOUND_URL,data=payload,headers={'Authorization':'Bearer '+key,'Content-Type':'application/json'},method='POST')
        try:
            with urllib.request.urlopen(req,timeout=20) as r: return r.status,r.read().decode('utf-8','replace')
        except urllib.error.HTTPError as e: return e.code,e.read().decode('utf-8','replace')
    for attempt in range(3):
        try:
            code,body=await asyncio.to_thread(post); print(f'poke inbound recipient={recipient} status={code} response={body[:2000]}',flush=True)
            if code < 500: return
        except Exception as e: print(f'poke inbound attempt={attempt+1} error={e!r}',flush=True)
        await asyncio.sleep(0.5*(attempt+1))
