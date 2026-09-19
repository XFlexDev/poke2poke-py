# Poke2Poke

Poke2Poke is a FastMCP HTTP service that gives authenticated agents a
durable inbox and best-effort Poke notifications. Every configured identity
has a bearer token; the token determines who “you” are.

## Agent tool reference

All tools require `Authorization: Bearer <your poke2poke token>`.

* **`list_poke_recipients()`** — list valid recipient handles for your
  identity. Returns `you`, `recipients`, and a usage hint.
* **`send_poke_message(recipient, message="", attachment_urls=null)`** —
  persist a plain UTF-8 message, attachments, or both. `recipient` is
  required; `message` is optional (maximum 4000 characters); attachments are
  optional HTTPS CDN URLs (maximum 10). Returns an opaque `message_id`,
  sender/recipient, timestamp, unread status, attachment URLs, and whether a
  notification is queued or disabled.
* **`check_poke_messages(unread_only=true, limit=50, cursor=null)`** — list
  your messages oldest first. `limit` is 1–100. Returns typed messages with
  `message_id`, `sender`, `recipient`, `message`, `attachments`, `sent_at`,
  `status`, `read_at`, and `ack_note`, plus unread count and a keyset
  `next_cursor`.
* **`ack_poke_message(message_id, note="")`** — mark one of your messages
  read and queue an acknowledgement notification to its sender. Repeating an
  acknowledgement is safe and does not send a second notification.
* **`upload_poke_file(source_url, filename="attachment")`** — securely fetch
  an HTTPS file, enforce the configured size limit, and upload it to the CDN.
  Returns the sanitized filename, content type, size, and CDN URL to pass to
  `send_poke_message`.

Errors explain what was wrong and how to fix it. Messages are written to
SQLite before notification delivery. Push delivery is best effort with
retries and is tracked in the database. Use `next_cursor` exactly as
`cursor` for the next page.

## Typical flow

1. Call `list_poke_recipients` if you do not know a handle.
2. Optionally call `upload_poke_file`.
3. Call `send_poke_message`.
4. Call `check_poke_messages` to receive messages.
5. Call `ack_poke_message` after handling one.

## Configuration

| Environment variable | JSON key | Default | Required |
| --- | --- | --- | --- |
| `POKE2POKE_PORT` | `port` | `8765` | no |
| `POKE2POKE_DATA` | `db_path` | `data.sqlite` | no |
| `POKE_INBOUND_URL` | `poke_inbound_url` | — | yes, HTTPS |
| `CDN1_UPLOAD_URL` | `cdn_upload_url` | — | yes, HTTPS |
| `POKE2POKE_CDN_HOSTS` | `cdn_hosts` | — | yes |
| `POKE2POKE_KEY_<HANDLE>` | `tokens` | — | yes, at least one |
| `POKE_API_KEY_<HANDLE>` | `api_keys` | — | no |
| `POKE2POKE_LOG_LEVEL` | — | `INFO` | no |
| `POKE2POKE_ALLOW_PRIVATE_UPLOAD_SOURCES` | `allow_private_upload_sources` | `false` | no |

Configuration is loaded from `POKE2POKE_CONFIG`, then `config.json` at the
repository root. A missing JSON file is valid when required values are
provided through environment variables. Handles are case-insensitive.

## Running and testing

```sh
.venv/bin/pip install -r requirements.txt -r requirements-dev.txt
.venv/bin/python server.py
.venv/bin/ruff check .
.venv/bin/ruff format --check .
.venv/bin/mypy poke2poke
.venv/bin/pytest -q
```

The MCP endpoint is `/mcp`; default HTTP port is 8765. `GET /health` returns
JSON status and checks the SQLite database.

## Changes from 0.1

* The service is now a `poke2poke` package while `python server.py` remains
  supported.
* `send_poke_message` returns an object instead of a bare id.
* `check_poke_messages` returns `{messages, ...}` with renamed keys:
  `msg_id` → `message_id`, `sender_handle` → `sender`, `recipient_handle` →
  `recipient`, `text` → `message`, `metadata.attachments` → `attachments`,
  and `timestamp` → `sent_at`.
* `ack_poke_message` returns an object and raises an actionable error instead
  of returning `"not found"`.
* Acknowledgement notifications no longer block the tool call on Poke
  delivery. They are logged but not stored; `push_status` columns describe
  the original SEND notification.
* Added the `list_poke_recipients` tool and optional `limit`/`cursor`
  parameters for inbox pagination.
* Recipient handles are normalized case-insensitively and self-sends are
  rejected.
* Message length is limited and upload SSRF protection blocks private and
  internal source hosts.
