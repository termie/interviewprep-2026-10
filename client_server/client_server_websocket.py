"""WebSocket Client-Server Example (Interview-Ready using `websockets`).

Key Concepts:
1. Full-Duplex Bi-directional Communication: Both client and server can send messages
   at any time over a persistent TCP connection after an initial HTTP handshake.
2. Built-in Message Framing: WebSockets provide discrete text and binary frames natively.
3. Event-Driven Asynchronous I/O using Python's `asyncio`.
"""

import asyncio
import websockets

HOST = "127.0.0.1"
PORT = 8765
URI = f"ws://{HOST}:{PORT}"


# --- Server ---
async def echo_handler(websocket: websockets.WebSocketServerProtocol) -> None:
    """Handles messages for a connected client session."""
    async for message in websocket:
        print(f"[Server] Received message: '{message}'")
        await websocket.send(f"ACK: {message}")


# --- Client ---
async def run_client() -> None:
    """Connects to server, sends requests, and receives replies."""
    # proxy=None ensures local loopback does not route through environment HTTP proxies
    async with websockets.connect(URI, proxy=None) as ws:
        for msg in ["ping", "hello websocket", "bidirectional frame"]:
            await ws.send(msg)
            reply = await ws.recv()
            print(f"[Client] Sent '{msg}', received '{reply}'")


# --- Demonstration ---
async def main() -> None:
    # Start server
    async with websockets.serve(echo_handler, HOST, PORT):
        print(f"[Server] WebSocket server listening on {URI}...")
        # Run client round-trip
        await run_client()
        print("[Main] Demo complete, closing server.")


if __name__ == "__main__":
    asyncio.run(main())
