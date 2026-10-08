"""
Phase 1: Streaming Text-to-Speech (TTS) Interface.
Supports ElevenLabs WebSocket streaming (and Cartesia), plus offline simulation.
"""

import asyncio
import base64
import json
import logging
import math
import struct
import time
from typing import Any, AsyncGenerator, Optional

try:
    import aiohttp
except ImportError:
    aiohttp = None

from config import TTSConfig, config
from metrics import LatencyTracker

logger = logging.getLogger("tts_stream")


class ElevenLabsTTSStream:
    """
    Direct asynchronous WebSocket client for ElevenLabs Turbo v2.5 streaming API.
    Streams incremental text chunks and yields raw audio bytes in real-time.
    """

    def __init__(self, tts_config: Optional[TTSConfig] = None):
        self.cfg = tts_config or config.tts
        self.api_key = self.cfg.elevenlabs_api_key
        self.voice_id = self.cfg.elevenlabs_voice_id
        self.model_id = self.cfg.elevenlabs_model_id
        self.output_format = self.cfg.elevenlabs_output_format
        self.endpoint = (
            f"wss://api.elevenlabs.io/v1/text-to-speech/{self.voice_id}/stream-input?"
            f"model_id={self.model_id}&output_format={self.output_format}"
        )
        self.session: Optional[Any] = None
        self.ws: Optional[Any] = None

    async def connect(self) -> None:
        """Connect to ElevenLabs WebSocket and send initial BOS packet."""
        if aiohttp is None:
            raise ImportError("aiohttp is required for live ElevenLabs streaming. Run: pip install aiohttp")
        if not self.api_key:
            raise ValueError("ELEVENLABS_API_KEY is not set.")
        
        self.session = aiohttp.ClientSession()
        headers = {"xi-api-key": self.api_key}
        self.ws = await self.session.ws_connect(self.endpoint, headers=headers)

        # Send Beginning of Stream (BOS) packet
        bos_payload = {
            "text": " ",
            "voice_settings": {
                "stability": 0.5,
                "similarity_boost": 0.8,
            },
            "generation_config": {
                "chunk_length_schedule": [120, 160, 250, 290]
            },
        }
        await self.ws.send_json(bos_payload)
        logger.info("Connected to ElevenLabs Streaming WebSocket.")

    async def stream_text_to_audio(
        self,
        text_generator: AsyncGenerator[str, None],
        latency_tracker: Optional[LatencyTracker] = None,
    ) -> AsyncGenerator[bytes, None]:
        """
        Pushes streaming text clauses into ElevenLabs WebSocket and yields audio bytes.
        """
        if not self.ws:
            await self.connect()

        if latency_tracker:
            latency_tracker.mark_tts_start()

        is_first_audio_byte = True
        total_audio_bytes = 0

        # Receiver worker: continuously receives audio frames from ElevenLabs
        async def audio_receiver(output_queue: asyncio.Queue):
            nonlocal is_first_audio_byte, total_audio_bytes
            try:
                async for msg in self.ws:
                    if msg.type == aiohttp.WSMsgType.TEXT:
                        data = json.loads(msg.data)
                        b64_audio = data.get("audio")
                        if b64_audio:
                            audio_chunk = base64.b64decode(b64_audio)
                            if audio_chunk:
                                if is_first_audio_byte:
                                    is_first_audio_byte = False
                                    if latency_tracker:
                                        latency_tracker.mark_tts_first_byte()
                                total_audio_bytes += len(audio_chunk)
                                await output_queue.put(audio_chunk)

                        is_final = data.get("isFinal", False)
                        if is_final:
                            break
                    elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                        break
            except Exception as e:
                logger.error(f"Error in ElevenLabs audio receiver: {e}")
            finally:
                await output_queue.put(None)  # Sentinel

        output_queue: asyncio.Queue = asyncio.Queue()
        receiver_task = asyncio.create_task(audio_receiver(output_queue))

        # Sender worker: send text clauses as they arrive
        try:
            async for clause in text_generator:
                if self.ws and not self.ws.closed:
                    await self.ws.send_json({
                        "text": clause + " ",
                        "try_trigger_generation": True
                    })

            # Send End of Stream (EOS) packet
            if self.ws and not self.ws.closed:
                await self.ws.send_json({"text": ""})

            # Yield audio chunks from the receiver queue
            while True:
                chunk = await output_queue.get()
                if chunk is None:
                    break
                yield chunk

        finally:
            await receiver_task
            if latency_tracker:
                latency_tracker.mark_tts_complete(total_audio_bytes)

    async def close(self) -> None:
        if self.ws and not self.ws.closed:
            await self.ws.close()
        if self.session and not self.session.closed:
            await self.session.close()


class MockTTSStream:
    """
    Offline/Simulation TTS Stream.
    Simulates realistic TTFB (~120ms) and yields standard 16-bit 16kHz PCM audio frames.
    """

    def __init__(
        self,
        simulated_ttfb_ms: float = 120.0,
        sample_rate: int = 16000,
        pacing_factor: float = 1.0
    ):
        self.simulated_ttfb = simulated_ttfb_ms / 1000.0
        self.sample_rate = sample_rate
        self.pacing_factor = pacing_factor

    async def connect(self) -> None:
        pass

    async def stream_text_to_audio(
        self,
        text_generator: AsyncGenerator[str, None],
        latency_tracker: Optional[LatencyTracker] = None,
    ) -> AsyncGenerator[bytes, None]:
        if latency_tracker:
            latency_tracker.mark_tts_start()

        is_first_chunk = True
        total_audio_bytes = 0

        # Simulate TTFB before first audio frame
        await asyncio.sleep(self.simulated_ttfb)

        async for clause in text_generator:
            if not clause.strip():
                continue

            if is_first_chunk:
                is_first_chunk = False
                if latency_tracker:
                    latency_tracker.mark_tts_first_byte()

            # Estimate speech duration based on word count (~150 words per minute = 2.5 words/sec)
            words = clause.split()
            duration_sec = max(0.4, len(words) * 0.35)
            num_samples = int(self.sample_rate * duration_sec)

            # Generate gentle synthesized vocal waveform (fundamental frequency 180Hz)
            chunk_samples = 800  # 50ms per packet
            for i in range(0, num_samples, chunk_samples):
                samples_in_packet = min(chunk_samples, num_samples - i)
                raw_packet = bytearray()
                for s in range(samples_in_packet):
                    t = float(i + s) / self.sample_rate
                    # Gentle modulating harmonic tone
                    val = math.sin(2 * math.pi * 180 * t) * 0.7 + math.sin(2 * math.pi * 360 * t) * 0.3
                    int_val = int(val * 4000.0)
                    raw_packet.extend(struct.pack('<h', int_val))

                total_audio_bytes += len(raw_packet)
                yield bytes(raw_packet)
                # Pace the audio output to match playback
                if self.pacing_factor > 0:
                    await asyncio.sleep((samples_in_packet / self.sample_rate) * self.pacing_factor)

        if latency_tracker:
            latency_tracker.mark_tts_complete(total_audio_bytes)

    async def close(self) -> None:
        pass
