"""
Phase 3: Offline Replay Client and Latency Benchmark Tool.
Simulates real-time microphone streaming by feeding pre-recorded WAV audio files
through the WebSocket pipeline, measuring turnaround latency and saving the output.
"""

import argparse
import asyncio
import base64
import json
import logging
import os
import struct
import time
import urllib.parse
import wave
from pathlib import Path
from typing import Optional

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("replay_client")

WS_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"
OP_TEXT = 0x1
OP_BINARY = 0x2
OP_CLOSE = 0x8
OP_PING = 0x9
OP_PONG = 0xA


class ReplayWebSocketClient:
    """Async client implementation of RFC 6455 WebSocket client using asyncio Streams."""

    def __init__(self, host: str, port: int, path: str = "/ws/audio"):
        self.host = host
        self.port = port
        self.path = path
        self.reader: Optional[asyncio.StreamReader] = None
        self.writer: Optional[asyncio.StreamWriter] = None
        self.closed = False

    async def connect(self) -> None:
        """Establish TCP connection and perform WebSocket upgrade handshake."""
        self.reader, self.writer = await asyncio.open_connection(self.host, self.port)
        raw_key = os.urandom(16)
        sec_key = base64.b64encode(raw_key).decode("utf-8")

        request = (
            f"GET {self.path} HTTP/1.1\r\n"
            f"Host: {self.host}:{self.port}\r\n"
            f"Upgrade: websocket\r\n"
            f"Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {sec_key}\r\n"
            f"Sec-WebSocket-Version: 13\r\n\r\n"
        )
        self.writer.write(request.encode("utf-8"))
        await self.writer.drain()

        # Read handshake response
        response_lines = []
        while True:
            line = await self.reader.readline()
            response_lines.append(line.decode("utf-8", errors="ignore"))
            if line == b"\r\n" or not line:
                break

        status_line = response_lines[0] if response_lines else ""
        if "101" not in status_line:
            raise ConnectionError(f"Handshake failed: {status_line}")

        logger.info(f"Connected to Voice Assistant at ws://{self.host}:{self.port}{self.path}")

    async def send_frame(self, opcode: int, payload: bytes) -> None:
        """Send masked RFC 6455 frame to server (client MUST mask payloads)."""
        if self.closed or self.writer.is_closing():
            return

        header = bytearray()
        header.append(0x80 | (opcode & 0x0F))  # FIN bit + Opcode

        mask_key = os.urandom(4)
        length = len(payload)

        # MASK bit set (0x80)
        if length <= 125:
            header.append(0x80 | length)
        elif length <= 65535:
            header.append(0x80 | 126)
            header.extend(struct.pack("!H", length))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", length))

        header.extend(mask_key)

        # Mask payload
        masked_payload = bytearray(length)
        for i in range(length):
            masked_payload[i] = payload[i] ^ mask_key[i % 4]

        self.writer.write(header + masked_payload)
        await self.writer.drain()

    async def send_audio_chunk(self, chunk: bytes) -> None:
        """Send raw binary audio frame."""
        await self.send_frame(OP_BINARY, chunk)

    async def read_frame(self) -> Optional[tuple[int, bytes]]:
        """Read an incoming unmasked frame from server."""
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
            return None

        return (opcode, payload)

    async def close(self) -> None:
        self.closed = True
        try:
            await self.send_frame(OP_CLOSE, b"")
            self.writer.close()
            await self.writer.wait_closed()
        except Exception:
            pass


async def run_replay(
    audio_path: str,
    server_url: str = "ws://127.0.0.1:8000/ws/audio",
    chunk_ms: int = 50,
    output_audio_path: str = "data/received_response.wav",
) -> None:
    """
    Feeds audio_path into the streaming WebSocket server in real-time chunks,
    records the returned speech response, and benchmarks turnaround latency.
    """
    if not os.path.exists(audio_path):
        raise FileNotFoundError(f"Audio file not found: {audio_path}")

    # Parse server URL
    parsed = urllib.parse.urlparse(server_url)
    host = parsed.hostname or "127.0.0.1"
    port = parsed.port or 8000
    path = parsed.path or "/ws/audio"

    # Read source WAV file
    with wave.open(audio_path, "rb") as wf:
        sample_rate = wf.getframerate()
        channels = wf.getnchannels()
        sampwidth = wf.getsampwidth()
        pcm_bytes = wf.readframes(wf.getnframes())

    bytes_per_sample = sampwidth * channels
    samples_per_chunk = int(sample_rate * (chunk_ms / 1000.0))
    chunk_size_bytes = samples_per_chunk * bytes_per_sample

    total_duration_sec = len(pcm_bytes) / (sample_rate * bytes_per_sample)
    logger.info(f"Loaded '{audio_path}': {total_duration_sec:.2f}s, {sample_rate}Hz, chunk={chunk_ms}ms ({chunk_size_bytes}B)")

    client = ReplayWebSocketClient(host, port, path)
    await client.connect()

    received_audio_bytes = bytearray()
    telemetry_records = []
    t_speech_end = 0.0
    t_client_first_audio = 0.0
    first_audio_received = False
    finished_receiving = asyncio.Event()

    # Background task: listen for server responses
    async def receiver_loop():
        nonlocal first_audio_received, t_client_first_audio
        try:
            while not client.closed:
                frame = await client.read_frame()
                if frame is None:
                    break
                opcode, payload = frame
                if opcode == OP_BINARY:
                    if not first_audio_received:
                        first_audio_received = True
                        t_client_first_audio = time.perf_counter()
                    received_audio_bytes.extend(payload)
                elif opcode == OP_TEXT:
                    try:
                        data = json.loads(payload.decode("utf-8"))
                        if data.get("type") == "latency_metrics":
                            telemetry_records.append(data.get("data", {}))
                            finished_receiving.set()
                    except json.JSONDecodeError:
                        pass
        except Exception as e:
            logger.error(f"Error in receiver: {e}")
        finally:
            finished_receiving.set()

    recv_task = asyncio.create_task(receiver_loop())

    # Stream user audio at real-time speed
    logger.info("🎙️ Streaming audio to server at simulated microphone speed...")
    offset = 0
    total_len = len(pcm_bytes)
    while offset < total_len:
        end = min(offset + chunk_size_bytes, total_len)
        chunk = pcm_bytes[offset:end]
        offset = end

        await client.send_audio_chunk(chunk)
        await asyncio.sleep(chunk_ms / 1000.0)

    # User finished speaking: mark silence endpointing trigger
    t_speech_end = time.perf_counter()
    logger.info("✅ Audio replay finished. Awaiting assistant response...")

    # Wait for assistant response to complete or timeout
    try:
        await asyncio.wait_for(finished_receiving.wait(), timeout=8.0)
    except asyncio.TimeoutError:
        logger.warning("Timed out waiting for server completion.")

    await client.close()
    await recv_task

    # Compute Client-Side Latency
    client_turnaround_ms = 0.0
    if first_audio_received and t_speech_end > 0:
        client_turnaround_ms = (t_client_first_audio - t_speech_end) * 1000.0

    print("\n" + "=" * 80)
    print("                    REPLAY BENCHMARK TEST RESULTS                      ")
    print("=" * 80)
    print(f"Replay Input Audio        : {audio_path} ({total_duration_sec:.2f}s)")
    print(f"Received Audio Chunks     : {len(received_audio_bytes)} bytes")
    print(f"Client Measured Turnaround: {client_turnaround_ms:.1f} ms (User Silence -> 1st Audio Byte)")

    if telemetry_records:
        server_metrics = telemetry_records[-1].get("metrics_ms", {})
        print("-" * 80)
        print("Server-Side Telemetry Breakdown:")
        print(f"  • ASR Latency           : {server_metrics.get('asr_latency', 0.0):.1f} ms")
        print(f"  • LLM TTFT              : {server_metrics.get('llm_ttft', 0.0):.1f} ms")
        print(f"  • First Clause Ready    : {server_metrics.get('first_clause_ready', 0.0):.1f} ms")
        print(f"  • TTS TTFB              : {server_metrics.get('tts_ttfb', 0.0):.1f} ms")
        print(f"  • Total Perceived Time  : {server_metrics.get('perceived_turnaround', 0.0):.1f} ms")
    print("=" * 80 + "\n")

    # Save received audio
    if len(received_audio_bytes) > 0:
        out_path = Path(output_audio_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with wave.open(str(out_path), "wb") as out_wf:
            out_wf.setnchannels(1)
            out_wf.setsampwidth(2)
            out_wf.setframerate(sample_rate)
            out_wf.writeframes(received_audio_bytes)
        logger.info(f"💾 Saved assistant audio response to: {out_path.resolve()}")
    else:
        logger.warning("No audio bytes received from server.")


async def run_replay_direct(
    audio_path: str,
    chunk_ms: int = 50,
    output_audio_path: str = "data/received_response.wav",
) -> None:
    """
    Direct in-process replay mode. Feeds audio_path directly through
    PipelineOrchestrator without requiring an active external network socket.
    Ideal for local offline debugging, regression testing, and CI pipelines.
    """
    from orchestrator import PipelineOrchestrator

    if not os.path.exists(audio_path):
        raise FileNotFoundError(f"Audio file not found: {audio_path}")

    with wave.open(audio_path, "rb") as wf:
        sample_rate = wf.getframerate()
        channels = wf.getnchannels()
        sampwidth = wf.getsampwidth()
        pcm_bytes = wf.readframes(wf.getnframes())

    bytes_per_sample = sampwidth * channels
    samples_per_chunk = int(sample_rate * (chunk_ms / 1000.0))
    chunk_size_bytes = samples_per_chunk * bytes_per_sample
    total_duration_sec = len(pcm_bytes) / (sample_rate * bytes_per_sample)

    logger.info(f"[Direct Replay] Loaded '{audio_path}': {total_duration_sec:.2f}s, {sample_rate}Hz, chunk={chunk_ms}ms")

    received_audio_bytes = bytearray()
    telemetry_records = []
    first_audio_received = False
    t_client_first_audio = 0.0

    async def send_audio_to_client(chunk: bytes):
        nonlocal first_audio_received, t_client_first_audio
        if not first_audio_received:
            first_audio_received = True
            t_client_first_audio = time.perf_counter()
        received_audio_bytes.extend(chunk)

    async def send_telemetry_to_client(metrics_dict: dict):
        telemetry_records.append(metrics_dict)

    orchestrator = PipelineOrchestrator(
        send_audio_to_client=send_audio_to_client,
        send_telemetry_to_client=send_telemetry_to_client,
        use_mock=True,
    )
    orchestrator.tts.pacing_factor = 0.1
    await orchestrator.start()

    logger.info("🎙️ Streaming audio chunks into orchestrator at real-time pace...")
    offset = 0
    total_len = len(pcm_bytes)
    while offset < total_len:
        end = min(offset + chunk_size_bytes, total_len)
        chunk = pcm_bytes[offset:end]
        offset = end

        await orchestrator.push_client_audio(chunk)
        await asyncio.sleep(chunk_ms / 1000.0)

    t_speech_end = time.perf_counter()
    logger.info("✅ Audio feed complete. Waiting for turn completion...")

    # Await turn processing
    await asyncio.sleep(2.0)
    await orchestrator.stop()

    client_turnaround_ms = 0.0
    if first_audio_received and t_speech_end > 0:
        client_turnaround_ms = (t_client_first_audio - t_speech_end) * 1000.0

    print("\n" + "=" * 80)
    print("                DIRECT IN-PROCESS REPLAY BENCHMARK RESULTS             ")
    print("=" * 80)
    print(f"Replay Input Audio        : {audio_path} ({total_duration_sec:.2f}s)")
    print(f"Received Audio Chunks     : {len(received_audio_bytes)} bytes")
    print(f"Client Measured Turnaround: {client_turnaround_ms:.1f} ms (User Silence -> 1st Audio Byte)")

    if telemetry_records:
        server_metrics = telemetry_records[-1].get("metrics_ms", {})
        print("-" * 80)
        print("Pipeline Telemetry Breakdown:")
        print(f"  • ASR Latency           : {server_metrics.get('asr_latency', 0.0):.1f} ms")
        print(f"  • LLM TTFT              : {server_metrics.get('llm_ttft', 0.0):.1f} ms")
        print(f"  • First Clause Ready    : {server_metrics.get('first_clause_ready', 0.0):.1f} ms")
        print(f"  • TTS TTFB              : {server_metrics.get('tts_ttfb', 0.0):.1f} ms")
        print(f"  • Total Perceived Time  : {server_metrics.get('perceived_turnaround', 0.0):.1f} ms")
    print("=" * 80 + "\n")

    if len(received_audio_bytes) > 0:
        out_path = Path(output_audio_path)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with wave.open(str(out_path), "wb") as out_wf:
            out_wf.setnchannels(1)
            out_wf.setsampwidth(2)
            out_wf.setframerate(sample_rate)
            out_wf.writeframes(received_audio_bytes)
        logger.info(f"💾 Saved assistant audio response to: {out_path.resolve()}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replay pre-recorded audio through voice pipeline.")
    parser.add_argument("--file", default="data/sample_query.wav", help="Path to input .wav file")
    parser.add_argument("--url", default="ws://127.0.0.1:8000/ws/audio", help="WebSocket server URL")
    parser.add_argument("--chunk-ms", type=int, default=50, help="Chunk interval in milliseconds")
    parser.add_argument("--save-response", default="data/received_response.wav", help="Output .wav path")
    parser.add_argument("--direct", action="store_true", help="Run direct in-process replay without external socket")
    args = parser.parse_args()

    if args.direct:
        asyncio.run(
            run_replay_direct(
                audio_path=args.file,
                chunk_ms=args.chunk_ms,
                output_audio_path=args.save_response,
            )
        )
    else:
        try:
            asyncio.run(
                run_replay(
                    audio_path=args.file,
                    server_url=args.url,
                    chunk_ms=args.chunk_ms,
                    output_audio_path=args.save_response,
                )
            )
        except (ConnectionRefusedError, PermissionError) as e:
            logger.warning(f"Could not connect to {args.url} ({e}). Switching to direct in-process replay...")
            asyncio.run(
                run_replay_direct(
                    audio_path=args.file,
                    chunk_ms=args.chunk_ms,
                    output_audio_path=args.save_response,
                )
            )
