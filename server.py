"""Poke2Poke MCP service entrypoint."""
import logging
import os
from fastmcp import FastMCP
from config import PORT
from tools import register

logging.basicConfig(level=os.environ.get('POKE2POKE_LOG_LEVEL', 'INFO'),
                    format='%(asctime)s %(levelname)s %(name)s %(message)s')
mcp = register(FastMCP('poke2poke'))

if __name__ == '__main__':
    mcp.run(transport='streamable-http', host=os.environ.get('POKE2POKE_HOST', '0.0.0.0'), port=PORT)
