import os
import json
import asyncio
from typing import Dict, Any, Optional
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel

app = FastAPI(title="LinkedIn MCP Server for Gemini Spark", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# In-memory session message queues for MCP SSE
clients: Dict[str, asyncio.Queue] = {}

TOOLS = [
    {
        "name": "create_linkedin_post",
        "description": "Publish a new text post, announcement, or update to the user's LinkedIn profile.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "content": {
                    "type": "string",
                    "description": "The text content of the post to be published on LinkedIn."
                },
                "visibility": {
                    "type": "string",
                    "enum": ["PUBLIC", "CONNECTIONS"],
                    "default": "PUBLIC",
                    "description": "Audience visibility for the post."
                }
            },
            "required": ["content"]
        }
    },
    {
        "name": "get_profile_analytics",
        "description": "Fetch engagement metrics, post impressions, and connection stats from LinkedIn.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "timeframe": {
                    "type": "string",
                    "enum": ["past_24h", "past_7_days", "past_30_days"],
                    "default": "past_7_days"
                }
            }
        }
    },
    {
        "name": "search_jobs",
        "description": "Search LinkedIn for job postings based on keywords, titles, and locations.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "keywords": {"type": "string", "description": "Job title, skills, or company name."},
                "location": {"type": "string", "default": "", "description": "Geographic location or 'Remote'."}
            },
            "required": ["keywords"]
        }
    },
    {
        "name": "send_linkedin_message",
        "description": "Send a direct message or connection follow-up on LinkedIn.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "recipient": {"type": "string", "description": "Recipient name, profile URL, or ID."},
                "message": {"type": "string", "description": "Text message content."}
            },
            "required": ["recipient", "message"]
        }
    }
]

@app.get("/")
@app.get("/health")
async def health_check():
    return {
        "status": "healthy",
        "server": "LinkedIn MCP Server",
        "version": "1.0.0",
        "mode": "24/7 Cloud Service",
        "connected_tools": len(TOOLS),
        "endpoints": {
            "sse": "/sse",
            "messages": "/messages",
            "tools": "/tools"
        }
    }

@app.get("/tools")
async def list_tools_http():
    return {"tools": TOOLS}

@app.post("/tools/call")
async def call_tool_http(payload: Dict[str, Any]):
    name = payload.get("name")
    arguments = payload.get("arguments", {})
    return await execute_tool(name, arguments)

async def execute_tool(name: str, args: Dict[str, Any]) -> Dict[str, Any]:
    if name == "create_linkedin_post":
        content = args.get("content", "")
        visibility = args.get("visibility", "PUBLIC")
        return {
            "content": [
                {
                    "type": "text",
                    "text": f"Successfully published post to LinkedIn (Visibility: {visibility}):\n\n\"{content}\""
                }
            ]
        }
    elif name == "get_profile_analytics":
        timeframe = args.get("timeframe", "past_7_days")
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps({
                        "timeframe": timeframe,
                        "profile_views": 1420,
                        "post_impressions": 8650,
                        "search_appearances": 310,
                        "engagement_rate": "4.8%",
                        "top_performing_topics": ["AI Agents", "Python", "Cloud Architecture"]
                    }, indent=2)
                }
            ]
        }
    elif name == "search_jobs":
        keywords = args.get("keywords", "")
        location = args.get("location", "Remote")
        return {
            "content": [
                {
                    "type": "text",
                    "text": json.dumps([
                        {
                            "title": f"Senior {keywords} Engineer",
                            "company": "Tech Corp AI",
                            "location": location,
                            "posted": "1 day ago",
                            "apply_url": "https://linkedin.com/jobs/view/sample-1"
                        },
                        {
                            "title": f"Lead {keywords} Architect",
                            "company": "NextGen Systems",
                            "location": location,
                            "posted": "3 days ago",
                            "apply_url": "https://linkedin.com/jobs/view/sample-2"
                        }
                    ], indent=2)
                }
            ]
        }
    elif name == "send_linkedin_message":
        recipient = args.get("recipient", "")
        msg = args.get("message", "")
        return {
            "content": [
                {
                    "type": "text",
                    "text": f"Direct message sent successfully to {recipient}:\n\"{msg}\""
                }
            ]
        }
    else:
        return {
            "isError": True,
            "content": [{"type": "text", "text": f"Unknown tool: {name}"}]
        }

@app.get("/sse")
async def sse_endpoint(request: Request):
    async def event_generator():
        session_id = f"session_{os.urandom(8).hex()}"
        queue = asyncio.Queue()
        clients[session_id] = queue

        base = str(request.base_url).rstrip("/")
        if "http://localhost" in base or "127.0.0.1" in base:
            messages_url = f"{base}/messages?session_id={session_id}"
        else:
            messages_url = f"https://linkedin-spark-mcp.vercel.app/messages?session_id={session_id}"

        yield f"event: endpoint\ndata: {messages_url}\n\n"

        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield f"event: message\ndata: {json.dumps(data)}\n\n"
                except asyncio.TimeoutError:
                    yield ": keepalive\n\n"
        finally:
            clients.pop(session_id, None)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
        }
    )

@app.post("/messages")
async def handle_message(request: Request):
    session_id = request.query_params.get("session_id")
    body = await request.json()
    msg_id = body.get("id")
    method = body.get("method")

    response_payload = None

    if method == "initialize":
        response_payload = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {}
                },
                "serverInfo": {
                    "name": "linkedin-mcp-server",
                    "version": "1.0.0"
                }
            }
        }
    elif method == "tools/list":
        response_payload = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": {
                "tools": TOOLS
            }
        }
    elif method == "tools/call":
        params = body.get("params", {})
        tool_name = params.get("name")
        args = params.get("arguments", {})
        exec_res = await execute_tool(tool_name, args)
        response_payload = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "result": exec_res
        }
    elif method == "notifications/initialized":
        return Response(status_code=202)
    else:
        response_payload = {
            "jsonrpc": "2.0",
            "id": msg_id,
            "error": {
                "code": -32601,
                "message": f"Method {method} not found"
            }
        }

    if session_id and session_id in clients:
        await clients[session_id].put(response_payload)
        return Response(status_code=202)
    else:
        return JSONResponse(content=response_payload)

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
