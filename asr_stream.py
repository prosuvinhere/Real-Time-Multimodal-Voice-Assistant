"""
Phase 1: Streaming Automatic Speech Recognition (ASR) Interface.
Supports Deepgram Live WebSocket streaming with endpointing, as well as offline mock streaming.
"""

import asyncio
import json
import logging
from typing import Any, AsyncGenerator, Callable, Optional

try:
    import aiohttp
except ImportError:
    aiohttp = None

from config import ASRConfig, config
from metrics import LatencyTracker

logger = logging.getLogger("asr_stream")


class DeepgramASRStream:
    """
    Direct asynchronous WebSocket client for Deepgram Live Transcription API.
    Handles continuous linear16/16kHz PCM streaming, interim results, and endpointing.
    """

    def __init__(self, asr_config: Optional[ASRConfig] = None):
        self.cfg = asr_config or config.asr
        self.api_key = self.cfg.api_key
        self.endpoint = (
            f"wss://api.deepgram.com/v1/listen?"
            f"encoding={self.cfg.encoding}&"
            f"sample_rate={self.cfg.sample_rate}&"
            f"channels={self.cfg.channels}&"
            f"model={self.cfg.model}&"
            f"interim_results=true&"
            f"endpointing={self.cfg.endpointing_ms}&"
            f"smart_format=true"
        )
        self.ws: Optional[Any] = None
        self.session: Optional[Any] = None
        self.is_connected = False
        self._accumulated_transcript = []

    async def connect(self) -> None:
        """Establish WebSocket connection with Deepgram."""
        if aiohttp is None:
            raise ImportError("aiohttp is required for live Deepgram streaming. Run: pip install aiohttp")
        if not self.api_key:
            raise ValueError("DEEPGRAM_API_KEY is not set.")
        headers = {"Authorization": f"Token {self.api_key}"}
        self.session = aiohttp.ClientSession()
        self.ws = await self.session.ws_connect(self.endpoint, headers=headers)
        self.is_connected = True
        logger.info("Connected to Deepgram Live Streaming WebSocket.")

    async def send_audio(self, chunk: bytes) -> None:
        """Send raw PCM audio bytes to Deepgram."""
        if self.ws and not self.ws.closed:
            await self.ws.send_bytes(chunk)

    async def listen(
        self,
        latency_tracker: Optional[LatencyTracker] = None,
        on_interim_transcript: Optional[Callable[[str], None]] = None
    ) -> AsyncGenerator[str, None]:
        """
        Listen for Deepgram transcription frames.
        Yields complete speech-final transcripts when user finishes speaking.
        """
        if not self.ws:
            return

        async for msg in self.ws:
            if msg.type == aiohttp.WSMsgType.TEXT:
                data = json.loads(msg.data)

                # Check for transcript alternatives
                channel = data.get("channel", {})
                alternatives = channel.get("alternatives", [])
                if alternatives:
                    alt = alternatives[0]
                    transcript = alt.get("transcript", "").strip()
                    is_final = data.get("is_final", False)
                    speech_final = data.get("speech_final", False)

                    if transcript and on_interim_transcript:
                        on_interim_transcript(transcript)

                    if is_final and transcript:
                        self._accumulated_transcript.append(transcript)

                    # speech_final means Deepgram endpointing detected user stopped speaking
                    if speech_final:
                        if latency_tracker:
                            latency_tracker.mark_speech_end()

                        full_transcript = " ".join(self._accumulated_transcript).strip()
                        self._accumulated_transcript = []
                        if full_transcript:
                            if latency_tracker:
                                latency_tracker.mark_asr_final(full_transcript)
                            yield full_transcript

            elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                logger.warning("Deepgram WebSocket closed or encountered error.")
                break

    async def close(self) -> None:
        """Gracefully close the ASR stream."""
        self.is_connected = False
        if self.ws and not self.ws.closed:
            # Send close frame
            await self.ws.send_json({"type": "CloseStream"})
            await self.ws.close()
        if self.session and not self.session.closed:
            await self.session.close()


class MockASRStream:
    """
    Offline/Simulation ASR Stream.
    Simulates speech endpointing and realistic transcription delays without live API keys.
    """

    def __init__(self, simulated_delay_ms: float = 120.0):
        self.simulated_delay = simulated_delay_ms / 1000.0
        self.is_connected = True
        self.audio_received_bytes = 0
        self.queue: asyncio.Queue[bytes] = asyncio.Queue()
        self._mock_responses = [
            "What is the weather like in San Francisco right now?",
            "Can you tell me a quick joke about computers?",
            "What is the capital of France and what is it famous for?",
            "How does real-time streaming text-to-speech work?",
        ]
        self._turn_index = 0

    async def connect(self) -> None:
        self.is_connected = True
        logger.info("[Mock ASR] Connected in simulation mode.")

    async def send_audio(self, chunk: bytes) -> None:
        self.audio_received_bytes += len(chunk)
        await self.queue.put(chunk)

    async def listen(
        self,
        latency_tracker: Optional[LatencyTracker] = None,
        on_interim_transcript: Optional[Callable[[str], None]] = None
    ) -> AsyncGenerator[str, None]:
        """Simulate ASR endpointing upon receiving audio."""
        while self.is_connected:
            try:
                # Wait for audio chunks
                await asyncio.wait_for(self.queue.get(), timeout=0.35)
            except asyncio.TimeoutError:
                # If audio was received and silence follows, simulate endpointing!
                if self.audio_received_bytes > 3200:  # > 100ms of audio received
                    if latency_tracker:
                        latency_tracker.mark_speech_end()

                    await asyncio.sleep(self.simulated_delay)
                    transcript = self._mock_responses[self._turn_index % len(self._mock_responses)]
                    self._turn_index += 1
                    self.audio_received_bytes = 0

                    if latency_tracker:
                        latency_tracker.mark_asr_final(transcript)

                    if on_interim_transcript:
                        on_interim_transcript(transcript)

                    yield transcript
                continue

    async def close(self) -> None:
        self.is_connected = False
