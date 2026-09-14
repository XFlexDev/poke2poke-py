"""Poke2Poke MCP service entrypoint."""
from config import PORT
from fastmcp import FastMCP
from tools import register
mcp=register(FastMCP('poke2poke'))
if __name__=='__main__': mcp.run(transport='streamable-http',host='0.0.0.0',port=PORT)
