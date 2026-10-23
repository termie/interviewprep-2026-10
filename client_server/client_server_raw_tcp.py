"""Raw TCP Client-Server Example (Interview-Ready).

Key Concepts:
1. Standard BSD sockets using `socket.socket`.
2. Multi-threaded server: accepts connections in a loop and spawns a thread per client.
3. Message Framing / Stream Demarcation: TCP is a continuous byte stream without
   inherent message boundaries. We use newline-delimited framing (`\\n`) with
   `socket.makefile()` to handle line-buffering cleanly.
"""

import socket
import threading
import time

HOST = "127.0.0.1"
PORT = 9999


# --- Server ---
def handle_client(conn: socket.socket, addr: tuple[str, int]) -> None:
    """Handles an individual client connection using line-buffered streams."""
    with conn, conn.makefile("r", encoding="utf-8") as reader, conn.makefile("w", encoding="utf-8") as writer:
        for line in reader:
            msg = line.strip()
            if not msg or msg == "QUIT":
                break
            print(f"[Server] Received from {addr}: '{msg}'")
            writer.write(f"ACK: {msg}\n")
            writer.flush()


def run_server(stop_event: threading.Event) -> None:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen(5)
    server.settimeout(0.5)  # Allows periodic check of stop_event

    print(f"[Server] Listening on {HOST}:{PORT}...")
    while not stop_event.is_set():
        try:
            conn, addr = server.accept()
            threading.Thread(target=handle_client, args=(conn, addr), daemon=True).start()
        except socket.timeout:
            continue

    server.close()


# --- Client ---
def send_request(message: str) -> str:
    """Connects, sends a framed request, and returns the server's reply."""
    with socket.create_connection((HOST, PORT)) as sock:
        with sock.makefile("r", encoding="utf-8") as reader, sock.makefile("w", encoding="utf-8") as writer:
            writer.write(f"{message}\n")
            writer.flush()
            response = reader.readline().strip()
            return response


# --- Demonstration ---
def main() -> None:
    stop_event = threading.Event()
    server_thread = threading.Thread(target=run_server, args=(stop_event,), daemon=True)
    server_thread.start()
    time.sleep(0.1)  # Allow server socket to bind

    # Send test messages from client
    for msg in ["ping", "hello world", "interview test"]:
        resp = send_request(msg)
        print(f"[Client] Sent '{msg}', received '{resp}'")

    stop_event.set()
    server_thread.join()
    print("[Main] Server stopped.")


if __name__ == "__main__":
    main()
