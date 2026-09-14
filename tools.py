import asyncio, json, mimetypes, os, secrets, urllib.parse, urllib.request
from datetime import datetime, timezone
from fastmcp import Context
from fastmcp.server.dependencies import get_http_request
from config import TOKENS, CDN_UPLOAD_URL, CDN_HOSTS
from db import connection
from poke_client import push_to_poke

async def caller(ctx: Context=None):
    try: auth=get_http_request().headers.get('authorization','')
    except Exception: auth=''
    if not auth.startswith('Bearer '): raise ValueError('unauthorized: Authorization: Bearer <configured key> required')
    token=auth[7:]
    for who,key in TOKENS.items():
        if key and secrets.compare_digest(token,key): return who
    raise ValueError('unauthorized: invalid bearer token')

def register(mcp):
 @mcp.tool()
 async def send_poke_message(recipient:str,message:str='',attachment_urls:list[str]=None,ctx:Context=None)->str:
    sender=await caller(ctx); recipient=recipient.strip(); urls=attachment_urls or []
    if recipient not in TOKENS or (not message.strip() and not urls): raise ValueError('recipient and message or attachment_urls are required')
    for url in urls:
        p=urllib.parse.urlparse(url)
        if p.scheme!='https' or (p.hostname or '').lower() not in CDN_HOSTS: raise ValueError('attachments must be HTTPS CDN1 URLs')
    mid=secrets.token_urlsafe(16); ts=datetime.now(timezone.utc).isoformat(); meta=json.dumps({'attachments':urls},separators=(',',':'))
    with connection() as c: c.execute('INSERT INTO messages VALUES (?,?,?,?,?,?,?)',(mid,sender,recipient,message,meta,ts,'unread'))
    asyncio.create_task(push_to_poke(recipient,message,sender,attachments=[{'url':u} for u in urls] or None)); return mid
 @mcp.tool()
 async def check_poke_messages(unread_only:bool=True,ctx:Context=None)->list[dict]:
    who=await caller(ctx); q='SELECT msg_id,sender_handle,recipient_handle,text,metadata,timestamp,status FROM messages WHERE recipient_handle=?'; args=[who]
    if unread_only:q+=" AND status != 'read'"
    q+=' ORDER BY timestamp ASC LIMIT 100'
    with connection() as c:
      out=[]
      for r in c.execute(q,args):
       d=dict(r)
       try:d['metadata']=json.loads(d['metadata'] or '{}')
       except json.JSONDecodeError:d['metadata']={}
       out.append(d)
      return out
 @mcp.tool()
 async def ack_poke_message(message_id:str,note:str='',ctx:Context=None)->str:
    who=await caller(ctx); note=(note or '').strip()
    with connection() as c:
      row=c.execute('SELECT sender_handle FROM messages WHERE msg_id=? AND recipient_handle=?',(message_id,who)).fetchone()
      if not row:return 'not found'
      c.execute("UPDATE messages SET status='read' WHERE msg_id=? AND recipient_handle=?",(message_id,who))
    await push_to_poke(row['sender_handle'],f'{who} acknowledged your message'+(f': {note}' if note else ''),who,event='acknowledgment',note=note,status='acknowledged'); return 'ok'
 @mcp.tool()
 async def upload_poke_file(source_url:str,filename:str='attachment',ctx:Context=None)->dict:
    """Fetch an HTTPS file in memory and upload it to CDN1."""
    await caller(ctx)
    p=urllib.parse.urlparse(source_url)
    if p.scheme!='https' or not p.hostname: raise ValueError('source_url must be HTTPS')
    def upload():
        with urllib.request.urlopen(source_url,timeout=60) as r:
            data=r.read(100*1024*1024+1); ctype=r.headers.get_content_type()
        if len(data)>100*1024*1024: raise ValueError('file exceeds 100 MiB limit')
        safe_name=os.path.basename(filename) or 'attachment'; boundary='----poke2poke-'+secrets.token_hex(16)
        body=(f'--{boundary}\r\nContent-Disposition: form-data; name="file"; filename="{safe_name}"\r\nContent-Type: {ctype or mimetypes.guess_type(safe_name)[0] or "application/octet-stream"}\r\n\r\n').encode()+data+f'\r\n--{boundary}--\r\n'.encode()
        req=urllib.request.Request(CDN_UPLOAD_URL,data=body,headers={'Content-Type':f'multipart/form-data; boundary={boundary}'},method='POST')
        with urllib.request.urlopen(req,timeout=120) as r: result=json.loads(r.read().decode())
        url=result.get('url') or result.get('cdn_url') or result.get('fileUrl'); up=urllib.parse.urlparse(url or '')
        if up.scheme!='https' or (up.hostname or '').lower() not in CDN_HOSTS: raise ValueError('CDN1 upload did not return a valid CDN1 URL')
        return {'url':url,'filename':filename,'content_type':ctype,'size':len(data)}
    return await asyncio.to_thread(upload)
 return mcp
