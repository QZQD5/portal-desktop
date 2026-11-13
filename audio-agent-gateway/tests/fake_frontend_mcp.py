from mcp.server.fastmcp import FastMCP

mcp = FastMCP("frontend-fixture")


@mcp.tool()
def echo(value: str) -> dict:
    return {"echo": value}


@mcp.tool()
def not_enabled() -> str:
    return "should not be exposed"


if __name__ == "__main__":
    mcp.run(transport="stdio")
