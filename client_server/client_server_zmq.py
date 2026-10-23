"""ZeroMQ Client-Server Example (REQ / REP Pattern).

Key Concepts:
1. Message-Oriented Framing: ZeroMQ automatically frames discrete messages,
   eliminating the byte-stream buffering/chunking problems of raw TCP.
2. Connection Agnostic: Client can connect before the server even starts; ZeroMQ
   handles queueing and reconnects transparently.
3. Strict Lockstep (REQ/REP): In the basic REQ/REP pattern, calls alternate strictly:
   Client does send -> recv; Server does recv -> send.
   (For asynchronous multi-client scale, ROUTER/DEALER is used instead).
"""

import threading
import time

try:
    import zmq
except ImportError:
    zmq = None

ADDRESS = "tcp://127.0.0.1:5555"


# --- Server ---
def run_server(stop_event: threading.Event) -> None:
    context = zmq.Context()
    socket = context.socket(zmq.REP)
    socket.bind(ADDRESS)
    socket.setsockopt(zmq.RCVTIMEO, 500)  # 500ms timeout to allow checking stop_event

    print(f"[Server] ZeroMQ REP listening on {ADDRESS}...")
    while not stop_event.is_set():
        try:
            msg = socket.recv_string()
            print(f"[Server] Received: '{msg}'")
            socket.send_string(f"ACK: {msg}")
        except zmq.Again:
            continue

    socket.close()
    context.term()


# --- Client ---
def run_client() -> None:
    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.connect(ADDRESS)

    for msg in ["ping", "hello zmq", "fast & framed"]:
        socket.send_string(msg)
        reply = socket.recv_string()
        print(f"[Client] Sent '{msg}', received '{reply}'")

    socket.close()
    context.term()


# --- Demonstration ---
def main() -> None:
    if zmq is None:
        print("Note: 'pyzmq' is not installed in the active environment.")
        print("To run this demo: pip install pyzmq")
        return

    stop_event = threading.Event()
    server_thread = threading.Thread(target=run_server, args=(stop_event,), daemon=True)
    server_thread.start()
    time.sleep(0.1)  # Allow socket to bind

    run_client()

    stop_event.set()
    server_thread.join()
    print("[Main] Server stopped.")


if __name__ == "__main__":
    main()
