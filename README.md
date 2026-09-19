# Poke2Poke Python

A small FastMCP HTTP service that lets configured Poke identities send, read, acknowledge, and upload messages.

## Setup

Requires Python 3.11+.

```sh
python -m venv venv
. venv/bin/activate
pip install -r requirements.txt
cp config.example.json config.json
# edit config.json with local, private values
python server.py
```

The server listens on port 8765 by default and uses FastMCP's streamable HTTP transport. Set `POKE2POKE_PORT` to change the port.

## Configuration

`config.json` contains runtime defaults and must never be committed. `.env` is also ignored. Environment variables override matching settings; see `.env.example`. Set `POKE2POKE_DATA` to choose the SQLite path and `POKE2POKE_CONFIG` to point at a different config file (the service runs from env vars alone if it is absent). SQLite databases and WAL files are intentionally ignored.

Identities are the keys of `tokens` (bearer tokens callers present) and `api_keys` (Poke inbound keys used to push to that identity). `POKE2POKE_KEY_<HANDLE>` / `POKE_API_KEY_<HANDLE>` env vars add or override identities.

Optional limits: `max_message_chars` (20000), `max_attachments` (10), `max_upload_bytes` (100 MiB); `cdn_hosts` may also be given as a comma-separated `CDN1_HOSTS` env var. `POKE2POKE_LOG_LEVEL` and `POKE2POKE_HOST` control logging and the bind address.

## Tools

- `send_poke_message(recipient, message='', attachment_urls=[])` — stores the message and pushes it to the recipient's Poke inbound URL in the background. Returns the message id.
- `check_poke_messages(unread_only=True, limit=100)` — lists the caller's messages, oldest first.
- `ack_poke_message(message_id, note='')` — marks a message read and notifies the sender once; repeat calls return `already acknowledged`.
- `upload_poke_file(source_url, filename)` — fetches a public HTTPS URL (private/loopback hosts and non-HTTPS redirects are rejected) and uploads it to CDN1.

## Tests

```sh
pip install pytest
python -m pytest -q tests
```

Never put production JWTs, Poke API keys, database files, or other credentials in source control. Rotate any credential that may have been exposed.

## License

MIT.
