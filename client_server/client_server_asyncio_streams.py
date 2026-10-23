"""Asyncio TCP Streams Client-Server Example (Interview-Ready Standard Library).

Key Concepts:
1. Standard Library Async Networking: Uses `asyncio.start_server` and `asyncio.open_connection`.
2. Clean Streaming & Framing: Uses `await reader.readline()` to handle TCP byte chunking
   and line framing without external dependencies or thread pools.
3. Flow Control: `await writer.drain()` ensures buffers don't overflow when sending data.
"""

import asyncio

HOST = "127.0.0.1"
PORT = 8888


# --- Server ---
async def handle_client(
    reader: asyncio.StreamReader, writer: asyncio.StreamWriter
) -> None:
    addr = writer.get_extra_info("peername")
    while True:
        line = await reader.readline()
        if not line:
            break
        msg = line.decode().strip()
        print(f"[Server] Received from {addr}: '{msg}'")
        writer.write(f"ACK: {msg}\n".encode())
        await writer.drain()  # Flow control: wait until outgoing buffer is flushed

    writer.close()
    await writer.wait_closed()


# --- Client ---
async def send_request(message: str) -> str:
    reader, writer = await asyncio.open_connection(HOST, PORT)
    writer.write(f"{message}\n".encode())
    await writer.drain()

    reply = await reader.readline()
    writer.close()
    await writer.wait_closed()
    return reply.decode().strip()


# --- Demonstration ---
async def main() -> None:
    server = await asyncio.start_server(handle_client, HOST, PORT)
    print(f"[Server] Asyncio TCP stream server listening on {HOST}:{PORT}...")

    async with server:
        for msg in ["ping", "asyncio streams", "standard library"]:
            reply = await send_request(msg)
            print(f"[Client] Sent '{msg}', received '{reply}'")

        server.close()
        await server.wait_closed()
        print("[Main] Server stopped.")


if __name__ == "__main__":
    asyncio.run(main())
