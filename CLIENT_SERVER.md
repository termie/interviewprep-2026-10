# Client-Server Networking: Guide & Comparison

This document summarizes client-server networking patterns in Python across **Raw TCP (BSD sockets)**, **ZeroMQ**, **WebSockets**, and **Asyncio Streams**, highlighting interview trade-offs and message framing considerations.

GitHub Repository: [`termie/interviewprep-2026-10`](https://github.com/termie/interviewprep-2026-10)

---

## Comparison Matrix

| Protocol / Library | Message Framing | Transport Layer | Concurrency Model | Reconnection Handling | Primary Use Case |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **[Raw TCP (`socket`)](https://github.com/termie/interviewprep-2026-10/blob/main/client_server/client_server_raw_tcp.py)** | None (Continuous byte stream; requires manual `\n` or length-prefix) | TCP | Thread-per-client or `select` | Manual `connect()` loop | Low-level protocols, embedded devices, interviews |
| **[ZeroMQ (`pyzmq`)](https://github.com/termie/interviewprep-2026-10/blob/main/client_server/client_server_zmq.py)** | Automatic (Discrete message frames) | TCP / inproc / IPC | Internal background I/O threads | Automatic built-in transparent reconnect | High-throughput distributed messaging, RPC, pub/sub |
| **[WebSockets (`websockets`)](https://github.com/termie/interviewprep-2026-10/blob/main/client_server/client_server_websocket.py)** | Automatic (RFC 6455 text & binary frames) | TCP (upgraded from HTTP) | Single-threaded `asyncio` event loop | Handled by client library | Web browser to server, bi-directional event streaming |
| **[Asyncio Streams](https://github.com/termie/interviewprep-2026-10/blob/main/client_server/client_server_asyncio_streams.py)** | Stream with `readline()` or `readexactly()` | TCP | Single-threaded `asyncio` event loop | Manual `open_connection` loop | High-concurrency standard-library Python servers |

---

## 1. Raw TCP Sockets: The Message Framing Problem

### The Core Interview Trap
TCP is a **stream protocol**, not a message protocol. If a client writes two strings:
```python
sock.sendall(b"Hello")
sock.sendall(b"World")
```
The server's `sock.recv(1024)` might receive:
* A single merged chunk: `b"HelloWorld"` (packet coalescence / Nagle's algorithm)
* Two partial chunks: `b"Hel"` followed by `b"loWorld"` (packet fragmentation)

### The Solution: Stream Demarcation
In an interview, you must explicitly implement or explain message framing:
1. **Delimiter-based Framing (e.g. `\n`)**:
   Use Python's `socket.makefile()` to wrap raw sockets into standard buffered Python file streams:
   ```python
   with conn.makefile("r") as reader, conn.makefile("w") as writer:
       for line in reader:
           msg = line.strip()
           writer.write(f"ACK: {msg}\n")
           writer.flush()
   ```
2. **Length-Prefixed Framing (TLV - Type-Length-Value)**:
   Prepend each message with a fixed-size header (e.g., 4-byte big-endian integer `struct.pack(">I", len(payload))`). The receiver first reads 4 bytes, unpacks the length $N$, and then loops until exactly $N$ payload bytes arrive.

---

## 2. ZeroMQ: Message-Oriented Abstraction

ZeroMQ operates at a higher abstraction level than raw sockets:
* **Framed Messages**: Sockets send and receive complete, discrete messages (`recv_string()` / `send_string()`). You never have to slice or buffer byte chunks.
* **Connection Agnostic**: A client can `connect()` before the server has even started or bound its port. ZeroMQ queues messages in memory and automatically negotiates the connection when the peer comes online.
* **Patterns**:
  * **REQ / REP**: Strict ping-pong lockstep (Client: `send` $\rightarrow$ `recv`; Server: `recv` $\rightarrow$ `send`).
  * **ROUTER / DEALER**: Fully asynchronous multi-client RPC architecture where messages contain routing identities to avoid head-of-line blocking.
  * **PUB / SUB**: Broadcast pattern where publishers push topic-tagged messages to subscribed clients.

---

## 3. WebSockets: Bi-directional Full-Duplex

WebSockets initiate as a standard HTTP/1.1 request (`Upgrade: websocket`) and transition to a persistent, bidirectional binary protocol:
* **Port Compatibility**: Runs over standard HTTP (80) and HTTPS (443) ports, bypassing corporate firewalls and NAT gateways that block raw TCP ports.
* **Built-in Framing**: Supports text (UTF-8) and binary frames, ping/pong heartbeats, and graceful close frames (`1000 OK`).
* **Python Implementation**: With `websockets`, the server handler is an async generator:
  ```python
  async def handler(websocket):
      async for message in websocket:
          await websocket.send(f"ACK: {message}")
  ```

---

## 4. Asyncio Streams: Standard Library Non-Blocking TCP

`asyncio.start_server` and `asyncio.open_connection` provide high-concurrency non-blocking TCP without relying on OS thread pools:
* Handles thousands of concurrent connections on a single OS thread.
* `reader.readline()` neatly solves the newline framing problem asynchronously.
* `await writer.drain()` applies essential **backpressure / flow control** so outgoing memory buffers don't grow unbounded when sending faster than the network can transmit.
