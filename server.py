"""
Phase 1 & Phase 2: High-Performance Real-Time Voice WebSocket Server.
Ingests live audio, orchestrates ASR -> LLM -> TTS, and streams audio & latency metrics to client.
Supports both standard library asyncio (zero external deps) and FastAPI/WebSockets.
"""

import asyncio
import base64
import hashlib
import json
import logging
import struct
from typing import Optional

from config import config
from orchestrator import PipelineOrchestrator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
)
logger = logging.getLogger("voice_server")

# WebSocket RFC 6455 Constants
WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
OP_TEXT = 0x1
OP_BINARY = 0x2
OP_CLOSE = 0x8
OP_PING = 0x9
OP_PONG = 0xA


class StandaloneWebSocketConnection:
    """Lightweight RFC 6455 WebSocket Connection built directly on asyncio Streams."""

    def __init__(self, reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
        self.reader = reader
        self.writer = writer
        self.closed = False

    async def handshake(self) -> bool:
        """Perform HTTP Upgrade -> WebSocket handshake."""
        request_data = b""
        while not self.reader.at_eof():
            line = await self.reader.readline()
            request_data += line
            if line == b"\r\n" or not line:
                break

        headers = {}
        for line_str in request_data.decode("utf-8", errors="ignore").split("\r\n"):
            if ":" in line_str:
                k, v = line_str.split(":", 1)
                headers[k.strip().lower()] = v.strip()

        sec_key = headers.get("sec-websocket-key")
        if not sec_key:
            self.writer.write(b"HTTP/1.1 400 Bad Request\r\n\r\n")
            await self.writer.drain()
            return False

        accept_val = base64.b64encode(
            hashlib.sha1((sec_key + WS_GUID).encode("utf-8")).digest()
        ).decode("utf-8")

        response = (
            "HTTP/1.1 101 Switching Protocols\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Accept: {accept_val}\r\n\r\n"
        )
        self.writer.write(response.encode("utf-8"))
        await self.writer.drain()
        return True

    async def send_frame(self, opcode: int, payload: bytes) -> None:
        """Send an unmasked RFC 6455 frame to client."""
        if self.closed or self.writer.is_closing():
            return

        header = bytearray()
        header.append(0x80 | (opcode & 0x0F))  # FIN bit + Opcode

        length = len(payload)
        if length <= 125:
            header.append(length)
        elif length <= 65535:
            header.append(126)
            header.extend(struct.pack("!H", length))
        else:
            header.append(127)
            header.extend(struct.pack("!Q", length))

        self.writer.write(header + payload)
        await self.writer.drain()

    async def send_text(self, text: str) -> None:
        """Send text message (JSON telemetry, transcripts)."""
        await self.send_frame(OP_TEXT, text.encode("utf-8"))

    async def send_bytes(self, data: bytes) -> None:
        """Send raw binary audio frame (TTS audio bytes)."""
        await self.send_frame(OP_BINARY, data)

    async def read_frame(self) -> Optional[tuple[int, bytes]]:
        """Read and unmask an incoming RFC 6455 frame from client."""
        if self.closed:
            return None

        try:
            head = await self.reader.readexactly(2)
        except (asyncio.IncompleteReadError, ConnectionResetError):
            self.closed = True
            return None

        opcode = head[0] & 0x0F
        is_masked = bool(head[1] & 0x80)
        length = head[1] & 0x7F

        if length == 126:
            ext_len = await self.reader.readexactly(2)
            length = struct.unpack("!H", ext_len)[0]
        elif length == 127:
            ext_len = await self.reader.readexactly(8)
            length = struct.unpack("!Q", ext_len)[0]

        mask_key = b""
        if is_masked:
            mask_key = await self.reader.readexactly(4)

        payload = await self.reader.readexactly(length)
        if is_masked:
            unmasked = bytearray(length)
            for i in range(length):
                unmasked[i] = payload[i] ^ mask_key[i % 4]
            payload = bytes(unmasked)

        if opcode == OP_CLOSE:
            self.closed = True
            await self.send_frame(OP_CLOSE, b"")
            return None
        elif opcode == OP_PING:
            await self.send_frame(OP_PONG, payload)
            return await self.read_frame()

        return (opcode, payload)

    async def close(self) -> None:
        self.closed = True
        try:
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:
            pass


async def handle_client_session(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
    """Handle end-to-end streaming audio session for a connected WebSocket client."""
    conn = StandaloneWebSocketConnection(reader, writer)
    if not await conn.handshake():
        return

    logger.info("Client connected to real-time voice pipeline.")

    # Callbacks for orchestrator to send data back to client
    async def send_audio_to_client(chunk: bytes) -> None:
        await conn.send_bytes(chunk)

    async def send_telemetry_to_client(metrics_dict: dict) -> None:
        payload = json.dumps({"type": "latency_metrics", "data": metrics_dict})
        await conn.send_text(payload)

    orchestrator = PipelineOrchestrator(
        send_audio_to_client=send_audio_to_client,
        send_telemetry_to_client=send_telemetry_to_client,
    )
    await orchestrator.start()

    try:
        while not conn.closed:
            frame = await conn.read_frame()
            if frame is None:
                break

            opcode, payload = frame
            if opcode == OP_BINARY:
                # Streaming audio chunk from user microphone
                await orchestrator.push_client_audio(payload)
            elif opcode == OP_TEXT:
                # Text/JSON control message
                try:
                    msg = json.loads(payload.decode("utf-8"))
                    msg_type = msg.get("type")
                    if msg_type == "barge_in":
                        await orchestrator.handle_barge_in()
                    elif msg_type == "ping":
                        await conn.send_text(json.dumps({"type": "pong"}))
                except json.JSONDecodeError:
                    pass

    except Exception as e:
        logger.error(f"Error handling client connection: {e}", exc_info=True)
    finally:
        logger.info("Client disconnected. Cleaning up pipeline resources.")
        await orchestrator.stop()
        await conn.close()


async def run_server(host: str = "0.0.0.0", port: int = 8000) -> None:
    """Start standalone WebSocket server."""
    server = await asyncio.start_server(handle_client_session, host, port)
    logger.info(f"🚀 Real-Time Voice Server listening on ws://{host}:{port}/ws/audio")
    logger.info(f"Target Latency Budget: < {config.budgets.total_perceived_ms}ms total perceived turnaround")
    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    host = config.host
    port = config.port
    try:
        asyncio.run(run_server(host, port))
    except KeyboardInterrupt:
        logger.info("Server terminated by user.")
