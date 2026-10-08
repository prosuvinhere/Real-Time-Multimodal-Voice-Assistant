"""
Pipeline Orchestrator for Real-Time Multimodal Voice Assistant.
Implements Phase 1 (streaming pipeline), Phase 2 (latency tracking),
and Phase 3 (timeouts, circuit breaker, barge-in, graceful degradation).
"""

import asyncio
import logging
import uuid
from typing import AsyncGenerator, Callable, Coroutine, Optional

from asr_stream import DeepgramASRStream, MockASRStream
from config import config
from fallback_audio import fallback_provider
from llm_stream import MockLLMStream, StreamingLLMClient
from metrics import LatencyTracker
from tts_stream import ElevenLabsTTSStream, MockTTSStream

logger = logging.getLogger("orchestrator")


class PipelineOrchestrator:
    """
    Coordinates real-time bi-directional streaming between Client, ASR, LLM, and TTS.
    Manages latency budgets, task cancellation on interruption, timeouts, and fallback audio.
    """

    def __init__(
        self,
        send_audio_to_client: Callable[[bytes], Coroutine],
        send_telemetry_to_client: Optional[Callable[[dict], Coroutine]] = None,
        use_mock: Optional[bool] = None,
    ):
        self.send_audio_to_client = send_audio_to_client
        self.send_telemetry_to_client = send_telemetry_to_client
        self.use_mock = config.mock_apis if use_mock is None else use_mock

        # Initialize upstream clients
        if self.use_mock or not config.asr.api_key:
            self.asr = MockASRStream()
        else:
            self.asr = DeepgramASRStream()

        if self.use_mock or not config.llm.api_key:
            self.llm = MockLLMStream()
        else:
            self.llm = StreamingLLMClient()

        if self.use_mock or not config.tts.elevenlabs_api_key:
            self.tts = MockTTSStream()
        else:
            self.tts = ElevenLabsTTSStream()

        self.fallback = fallback_provider

        # State management
        self.is_running = False
        self.is_assistant_speaking = False
        self._consecutive_failures = 0
        self._current_turn_task: Optional[asyncio.Task] = None
        self._asr_listener_task: Optional[asyncio.Task] = None
        self.conversation_history = []

    async def start(self) -> None:
        """Start upstream connections and ASR background listener."""
        self.is_running = True
        await self.asr.connect()
        await self.tts.connect()

        # Start ASR listener loop in background
        self._asr_listener_task = asyncio.create_task(self._asr_listener_loop())
        logger.info(f"Orchestrator started (Mock Mode: {self.use_mock}).")

    async def _asr_listener_loop(self) -> None:
        """Continuously processes ASR events and dispatches user speech turns."""
        try:
            tracker = LatencyTracker(request_id=f"req_{uuid.uuid4().hex[:8]}")
            async for transcript in self.asr.listen(latency_tracker=tracker):
                if not transcript.strip():
                    continue

                logger.info(f"User Final Transcript: '{transcript}'")

                # Interrupt / Barge-in: cancel any previous assistant response in-flight
                await self.handle_barge_in()

                # Launch downstream LLM -> TTS processing task
                self._current_turn_task = asyncio.create_task(
                    self._process_turn(transcript, tracker)
                )

                # Prepare new tracker for subsequent turn
                tracker = LatencyTracker(request_id=f"req_{uuid.uuid4().hex[:8]}")

        except asyncio.CancelledError:
            pass
        except Exception as e:
            logger.error(f"Error in ASR listener loop: {e}", exc_info=True)

    async def push_client_audio(self, audio_chunk: bytes) -> None:
        """
        Ingest streaming microphone audio chunk from client.
        Triggers barge-in detection if assistant was currently speaking.
        """
        if self.is_assistant_speaking and len(audio_chunk) > 0:
            # User interrupted the assistant while audio was playing!
            await self.handle_barge_in()

        await self.asr.send_audio(audio_chunk)

    async def handle_barge_in(self) -> None:
        """Interrupt active response and clear audio playback queues."""
        if self._current_turn_task and not self._current_turn_task.done():
            logger.info("⚡ Barge-in detected! Aborting active response generation.")
            self._current_turn_task.cancel()
            try:
                await self._current_turn_task
            except asyncio.CancelledError:
                pass
            self._current_turn_task = None

        self.is_assistant_speaking = False

    async def _process_turn(self, transcript: str, tracker: LatencyTracker) -> None:
        """
        Phase 1 & Phase 3 Core Turn Execution:
        Orchestrates LLM streaming, clause chunking, TTS audio streaming,
        enforcing strict TTFT and TTFB timeouts, and triggering graceful degradation if needed.
        """
        self.is_assistant_speaking = True
        request_id = tracker.request_id

        # Phase 3: Circuit Breaker Check
        if self._consecutive_failures >= config.timeouts.circuit_breaker_threshold:
            logger.warning("🚨 Circuit Breaker OPEN! Upstream services repeatedly failing. Serving fallback audio.")
            await self._trigger_graceful_degradation(tracker, "Circuit breaker open")
            return

        try:
            # 1. Start LLM Streaming with TTFT Timeout
            llm_text_stream = self._stream_llm_with_timeout(transcript, tracker)

            # 2. Feed LLM clauses directly into TTS with TTFB Timeout
            tts_audio_stream = self._stream_tts_with_timeout(llm_text_stream, tracker)

            # 3. Stream audio chunks to client in real-time
            is_first_chunk = True
            async for audio_chunk in tts_audio_stream:
                if is_first_chunk:
                    is_first_chunk = False
                    tracker.mark_client_first_audio_sent()

                await self.send_audio_to_client(audio_chunk)

            # Success: reset circuit breaker counter
            self._consecutive_failures = 0

        except asyncio.CancelledError:
            logger.info(f"Turn {request_id} cancelled by user interruption.")
            raise

        except (asyncio.TimeoutError, Exception) as err:
            self._consecutive_failures += 1
            logger.error(
                f"❌ Error during turn processing ({type(err).__name__}: {err}). "
                f"Consecutive failures: {self._consecutive_failures}. Activating Graceful Degradation."
            )
            await self._trigger_graceful_degradation(tracker, str(err))

        finally:
            self.is_assistant_speaking = False

            # Phase 2: Emit Latency Breakdown and Telemetry
            tracker.print_summary()
            if self.send_telemetry_to_client:
                await self.send_telemetry_to_client(tracker.to_dict())

    async def _stream_llm_with_timeout(
        self, transcript: str, tracker: LatencyTracker
    ) -> AsyncGenerator[str, None]:
        """Wrap LLM streaming with strict Time-to-First-Token timeout budget."""
        llm_gen = self.llm.stream_completion(
            user_prompt=transcript,
            conversation_history=self.conversation_history[-4:],
            latency_tracker=tracker,
        )

        # Enforce strict TTFT timeout on the first token/clause
        try:
            first_clause = await asyncio.wait_for(
                llm_gen.__anext__(),
                timeout=config.timeouts.llm_ttft_sec
            )
            yield first_clause
        except StopAsyncIteration:
            return
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError(
                f"LLM TTFT exceeded timeout limit of {config.timeouts.llm_ttft_sec}s!"
            )

        # Yield remainder of clauses
        async for clause in llm_gen:
            yield clause

    async def _stream_tts_with_timeout(
        self, text_generator: AsyncGenerator[str, None], tracker: LatencyTracker
    ) -> AsyncGenerator[bytes, None]:
        """Wrap TTS streaming with strict Time-to-First-Byte timeout budget."""
        tts_gen = self.tts.stream_text_to_audio(
            text_generator=text_generator,
            latency_tracker=tracker
        )

        # Enforce strict TTFB timeout on the first audio frame
        try:
            first_chunk = await asyncio.wait_for(
                tts_gen.__anext__(),
                timeout=config.timeouts.tts_ttfb_sec
            )
            yield first_chunk
        except StopAsyncIteration:
            return
        except asyncio.TimeoutError:
            raise asyncio.TimeoutError(
                f"TTS TTFB exceeded timeout limit of {config.timeouts.tts_ttfb_sec}s!"
            )

        # Yield remainder of audio frames
        async for chunk in tts_gen:
            yield chunk

    async def _trigger_graceful_degradation(self, tracker: LatencyTracker, reason: str) -> None:
        """Stream pre-recorded/synthesized fallback audio when services fail or hang."""
        tracker.mark_fallback()
        logger.info(f"📢 Streaming pre-recorded fallback audio to client. Reason: {reason}")

        is_first_chunk = True
        total_fallback_bytes = 0

        async for chunk in self.fallback.stream_audio_chunks():
            if is_first_chunk:
                is_first_chunk = False
                tracker.mark_client_first_audio_sent()

            total_fallback_bytes += len(chunk)
            await self.send_audio_to_client(chunk)

        tracker.mark_tts_complete(total_fallback_bytes)

    async def stop(self) -> None:
        """Gracefully stop orchestrator and connections."""
        self.is_running = False
        if self._current_turn_task:
            self._current_turn_task.cancel()
        if self._asr_listener_task:
            self._asr_listener_task.cancel()

        await self.asr.close()
        await self.tts.close()
        logger.info("Orchestrator stopped.")
