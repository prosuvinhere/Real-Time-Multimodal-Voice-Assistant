"""
Comprehensive Offline Test Suite for Real-Time Multimodal Voice Assistant.
Tests Phase 1 (streaming pipeline), Phase 2 (latency tracking),
and Phase 3 (timeouts, circuit breaker, barge-in, fallback audio).
"""

import asyncio
import os
import time
import unittest

from chunker import SentenceChunker
from config import config
from fallback_audio import FallbackAudioProvider, generate_synthesized_chime_wav
from metrics import LatencyTracker
from orchestrator import PipelineOrchestrator


class TestVoicePipeline(unittest.IsolatedAsyncioTestCase):

    def test_sentence_chunker(self):
        """Test streaming sentence/clause splitting."""
        chunker = SentenceChunker(min_clause_chars=20)
        tokens = ["Hello", "there!", "How", "can", "I", "assist", "you", "today?", "Let's", "talk."]

        chunks = []
        for token in tokens:
            chunks.extend(chunker.process_token(token + " "))
        chunks.extend(chunker.flush())

        self.assertGreaterEqual(len(chunks), 2)
        self.assertIn("Hello there!", chunks[0])
        print(f"✅ Chunker successfully produced {len(chunks)} speech units: {chunks}")

    def test_latency_tracker(self):
        """Test high-resolution latency tracking and budget verification."""
        tracker = LatencyTracker("test_req_001")
        tracker.mark_speech_end()
        time.sleep(0.05)
        tracker.mark_asr_final("Hello world")
        tracker.mark_llm_start()
        time.sleep(0.04)
        tracker.mark_llm_first_token()
        tracker.mark_first_clause_ready()
        tracker.mark_tts_start()
        time.sleep(0.03)
        tracker.mark_tts_first_byte()
        tracker.mark_client_first_audio_sent()
        tracker.mark_llm_complete("Hello world from assistant.")
        tracker.mark_tts_complete(3200)

        data = tracker.to_dict()
        m = data["metrics_ms"]
        self.assertGreater(m["asr_latency"], 30.0)
        self.assertGreater(m["llm_ttft"], 20.0)
        self.assertGreater(m["tts_ttfb"], 20.0)
        self.assertGreater(m["perceived_turnaround"], 80.0)

        summary_text = tracker.print_summary()
        self.assertIn("LATENCY BUDGET BREAKDOWN REPORT", summary_text)
        print("✅ LatencyTracker correctly recorded and formatted metrics.")

    def test_fallback_audio_synthesis(self):
        """Test fallback WAV audio generation and streaming."""
        test_wav = "static/test_chime.wav"
        generate_synthesized_chime_wav(test_wav, duration_sec=1.0)
        self.assertTrue(os.path.exists(test_wav))

        provider = FallbackAudioProvider(test_wav)
        async def read_chunks():
            count = 0
            async for chunk in provider.stream_audio_chunks(chunk_size=1600, delay_sec=0.001):
                count += len(chunk)
            return count

        total_bytes = asyncio.run(read_chunks())
        self.assertGreater(total_bytes, 0)
        print(f"✅ Fallback audio streamed {total_bytes} bytes successfully.")
        if os.path.exists(test_wav):
            os.remove(test_wav)

    async def test_end_to_end_streaming_pipeline(self):
        """Phase 1 & Phase 2: Test full orchestrator streaming with mock engines."""
        received_audio = bytearray()
        telemetry = []

        async def send_audio(chunk: bytes):
            received_audio.extend(chunk)

        async def send_telemetry(metrics: dict):
            telemetry.append(metrics)

        orchestrator = PipelineOrchestrator(
            send_audio_to_client=send_audio,
            send_telemetry_to_client=send_telemetry,
            use_mock=True,
        )
        orchestrator.tts.pacing_factor = 0.0
        await orchestrator.start()

        # Send simulated audio chunks
        dummy_audio = b"\x00\x01" * 8000  # 500ms of PCM audio
        await orchestrator.push_client_audio(dummy_audio)

        # Allow pipeline to process turn (0.35s silence + 0.12s ASR + 0.18s LLM + instantaneous mock TTS)
        await asyncio.sleep(1.2)
        await orchestrator.stop()

        self.assertGreater(len(received_audio), 0)
        self.assertGreater(len(telemetry), 0)
        last_turn = telemetry[-1]
        self.assertIn("metrics_ms", last_turn)
        self.assertFalse(last_turn["fallback_used"])
        print(f"✅ End-to-end pipeline completed. Received {len(received_audio)} audio bytes.")

    async def test_timeout_and_graceful_degradation(self):
        """Phase 3: Force upstream timeout and verify automatic fallback audio streaming."""
        received_audio = bytearray()
        telemetry = []

        async def send_audio(chunk: bytes):
            received_audio.extend(chunk)

        async def send_telemetry(metrics: dict):
            telemetry.append(metrics)

        orchestrator = PipelineOrchestrator(
            send_audio_to_client=send_audio,
            send_telemetry_to_client=send_telemetry,
            use_mock=True,
        )
        # Mock LLM that hangs forever to test strict timeout enforcement
        class HangingLLM:
            async def stream_completion(self, *args, **kwargs):
                await asyncio.sleep(10.0)
                yield "never reached"

        orchestrator.llm = HangingLLM()
        # Set a short timeout for the test
        config.timeouts.llm_ttft_sec = 0.2

        await orchestrator.start()
        tracker = LatencyTracker("timeout_test_req")
        tracker.mark_speech_end()

        # Run turn directly
        await orchestrator._process_turn("Hello test question", tracker)
        await orchestrator.stop()

        self.assertGreater(len(received_audio), 0)
        self.assertTrue(tracker.record.degraded_fallback_used)
        print("✅ Strict timeout successfully caught and degraded to fallback audio.")

    async def test_barge_in_interruption(self):
        """Phase 3: Test that client speaking interrupts assistant playback."""
        received_audio = bytearray()

        async def send_audio(chunk: bytes):
            received_audio.extend(chunk)

        orchestrator = PipelineOrchestrator(
            send_audio_to_client=send_audio,
            use_mock=True,
        )
        await orchestrator.start()

        # Simulate user speech to trigger assistant turn
        dummy_audio = b"\x01\x02" * 8000
        await orchestrator.push_client_audio(dummy_audio)
        await asyncio.sleep(0.5)

        # Simulate barge-in interruption while assistant is speaking
        self.assertTrue(orchestrator.is_assistant_speaking or orchestrator._current_turn_task is not None)
        await orchestrator.handle_barge_in()
        self.assertFalse(orchestrator.is_assistant_speaking)
        self.assertIsNone(orchestrator._current_turn_task)

        await orchestrator.stop()
        print("✅ Barge-in interruption successfully cancelled active turn.")


if __name__ == "__main__":
    unittest.main()
