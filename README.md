# linkedin-spark-mcp

A small [Model Context Protocol](https://modelcontextprotocol.io) (MCP) server skeleton, written with FastAPI, that exposes LinkedIn-style tools to an AI agent over HTTP and Server-Sent Events. It was built as a prototype for connecting an agent (e.g. Gemini Spark) to a LinkedIn workflow.

> **Status: prototype with mock tool implementations.** The server speaks the MCP transport correctly and advertises its tools, but the tool handlers return canned responses. They do **not** call LinkedIn's API, post anything, or send any messages. Wire the handlers in `execute_tool` to a real LinkedIn integration before using it for anything real.

## Tools

| Tool | Purpose | Current behaviour |
|---|---|---|
| `create_linkedin_post` | Publish a post (content, visibility) | Mock — echoes the post back |
| `get_profile_analytics` | Profile views, impressions, engagement | Mock — fixed sample numbers |
| `search_jobs` | Search jobs by keywords and location | Mock — two sample listings |
| `send_linkedin_message` | Send a direct message | Mock — echoes the message back |

## Endpoints

| Method | Path | Description |
|---|---|---|
| GET | `/.well-known/mcp.json`, `/mcp.json` | Server manifest |
| GET | `/`, `/health` | Health check |
| GET | `/tools` | List tools |
| POST | `/tools/call` | Call a tool over plain HTTP |
| GET | `/mcp`, `/sse` | SSE transport |
| POST | `/mcp`, `/messages` | JSON-RPC message endpoint |

## Run locally

```sh
pip install -r requirements.txt
python main.py                 # listens on $PORT, default 8000
```

```sh
curl http://localhost:8000/tools
curl -X POST http://localhost:8000/tools/call \
  -H "Content-Type: application/json" \
  -d '{"name":"search_jobs","arguments":{"keywords":"Python","location":"Remote"}}'
```

## Tech

Python · FastAPI · Uvicorn · Pydantic · httpx

## Next steps

- Replace the mock handlers with real LinkedIn API calls (OAuth, posting, messaging within LinkedIn's terms).
- Add authentication and restrict the permissive CORS settings before exposing it publicly.
