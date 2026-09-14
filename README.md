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

`config.json` contains runtime defaults and must never be committed. `.env` is also ignored. Environment variables override matching settings; see `.env.example`. Set `POKE2POKE_DATA` to choose the SQLite path. SQLite databases and WAL files are intentionally ignored.

Never put production JWTs, Poke API keys, database files, or other credentials in source control. Rotate any credential that may have been exposed.

## License

MIT.
