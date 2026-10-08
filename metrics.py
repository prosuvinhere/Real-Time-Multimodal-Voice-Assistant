"""
Phase 2: Latency Budget & Tracking Instrumentation.
Measures high-precision timestamps across ASR, LLM, TTS, and Network layers.
"""

import json
import logging
import time
from dataclasses import asdict, dataclass
from typing import Any, Dict, Optional

from config import LatencyBudgets, config

logger = logging.getLogger("latency_tracker")


@dataclass
class TurnLatencyRecord:
    """Detailed breakdown of latency for a single voice turn."""
    request_id: str
    
    # Raw Timestamps (time.perf_counter)
    t_speech_end: Optional[float] = None
    t_asr_final: Optional[float] = None
    t_llm_start: Optional[float] = None
    t_llm_ttft: Optional[float] = None
    t_first_clause: Optional[float] = None
    t_tts_start: Optional[float] = None
    t_tts_first_byte: Optional[float] = None
    t_client_first_audio: Optional[float] = None
    t_llm_complete: Optional[float] = None
    t_tts_complete: Optional[float] = None

    # Calculated Latencies (in milliseconds)
    asr_latency_ms: Optional[float] = None
    llm_ttft_ms: Optional[float] = None
    clause_latency_ms: Optional[float] = None
    tts_ttfb_ms: Optional[float] = None
    network_buffer_ms: Optional[float] = None
    perceived_turnaround_ms: Optional[float] = None
    llm_total_duration_ms: Optional[float] = None
    tts_total_duration_ms: Optional[float] = None

    # Text payloads
    transcript: str = ""
    llm_response_text: str = ""
    total_audio_bytes_sent: int = 0
    degraded_fallback_used: bool = False


class LatencyTracker:
    """Thread-safe / Async-safe high-resolution latency tracker per conversation turn."""

    def __init__(self, request_id: str, budgets: Optional[LatencyBudgets] = None):
        self.request_id = request_id
        self.budgets = budgets or config.budgets
        self.record = TurnLatencyRecord(request_id=request_id)

    def mark_speech_end(self, timestamp: Optional[float] = None) -> None:
        """Mark when user silence/endpointing was detected."""
        self.record.t_speech_end = timestamp or time.perf_counter()

    def mark_asr_final(self, transcript: str) -> None:
        """Mark when ASR returned the final transcription."""
        now = time.perf_counter()
        self.record.t_asr_final = now
        self.record.transcript = transcript
        if self.record.t_speech_end:
            self.record.asr_latency_ms = (now - self.record.t_speech_end) * 1000.0

    def mark_llm_start(self) -> None:
        """Mark when prompt was dispatched to LLM."""
        self.record.t_llm_start = time.perf_counter()

    def mark_llm_first_token(self) -> None:
        """Mark Time-to-First-Token (TTFT)."""
        now = time.perf_counter()
        self.record.t_llm_ttft = now
        if self.record.t_llm_start:
            self.record.llm_ttft_ms = (now - self.record.t_llm_start) * 1000.0

    def mark_first_clause_ready(self) -> None:
        """Mark when the first speakable sentence/clause is ready for TTS."""
        now = time.perf_counter()
        self.record.t_first_clause = now
        if self.record.t_llm_start:
            self.record.clause_latency_ms = (now - self.record.t_llm_start) * 1000.0

    def mark_tts_start(self) -> None:
        """Mark when first clause text was pushed to TTS."""
        self.record.t_tts_start = time.perf_counter()

    def mark_tts_first_byte(self) -> None:
        """Mark Time-to-First-Byte (TTFB) from TTS engine."""
        now = time.perf_counter()
        self.record.t_tts_first_byte = now
        if self.record.t_tts_start:
            self.record.tts_ttfb_ms = (now - self.record.t_tts_start) * 1000.0

    def mark_client_first_audio_sent(self) -> None:
        """Mark when first audio chunk is pushed over WebSocket to the client."""
        now = time.perf_counter()
        self.record.t_client_first_audio = now
        if self.record.t_tts_first_byte:
            self.record.network_buffer_ms = (now - self.record.t_tts_first_byte) * 1000.0
        if self.record.t_speech_end:
            self.record.perceived_turnaround_ms = (now - self.record.t_speech_end) * 1000.0

    def mark_llm_complete(self, response_text: str) -> None:
        """Mark when LLM finishes generating all tokens."""
        now = time.perf_counter()
        self.record.t_llm_complete = now
        self.record.llm_response_text = response_text
        if self.record.t_llm_start:
            self.record.llm_total_duration_ms = (now - self.record.t_llm_start) * 1000.0

    def mark_tts_complete(self, total_bytes: int) -> None:
        """Mark when all TTS audio frames have been streamed."""
        now = time.perf_counter()
        self.record.t_tts_complete = now
        self.record.total_audio_bytes_sent = total_bytes
        if self.record.t_tts_start:
            self.record.tts_total_duration_ms = (now - self.record.t_tts_start) * 1000.0

    def mark_fallback(self) -> None:
        """Mark that fallback audio was triggered due to upstream failure/timeout."""
        self.record.degraded_fallback_used = True

    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary for JSON serialization."""
        return {
            "request_id": self.record.request_id,
            "transcript": self.record.transcript,
            "llm_response": self.record.llm_response_text,
            "metrics_ms": {
                "asr_latency": round(self.record.asr_latency_ms or 0.0, 1),
                "llm_ttft": round(self.record.llm_ttft_ms or 0.0, 1),
                "first_clause_ready": round(self.record.clause_latency_ms or 0.0, 1),
                "tts_ttfb": round(self.record.tts_ttfb_ms or 0.0, 1),
                "network_buffer": round(self.record.network_buffer_ms or 0.0, 1),
                "perceived_turnaround": round(self.record.perceived_turnaround_ms or 0.0, 1),
                "llm_total_duration": round(self.record.llm_total_duration_ms or 0.0, 1),
                "tts_total_duration": round(self.record.tts_total_duration_ms or 0.0, 1),
            },
            "budgets_ms": {
                "budget_asr": self.budgets.asr_ms,
                "budget_llm_ttft": self.budgets.llm_ttft_ms,
                "budget_tts_ttfb": self.budgets.tts_ttfb_ms,
                "budget_perceived": self.budgets.total_perceived_ms,
            },
            "budget_compliance": {
                "asr_ok": (self.record.asr_latency_ms or 0.0) <= self.budgets.asr_ms,
                "llm_ttft_ok": (self.record.llm_ttft_ms or 0.0) <= self.budgets.llm_ttft_ms,
                "tts_ttfb_ok": (self.record.tts_ttfb_ms or 0.0) <= self.budgets.tts_ttfb_ms,
                "perceived_ok": (self.record.perceived_turnaround_ms or 0.0) <= self.budgets.total_perceived_ms,
            },
            "total_audio_bytes": self.record.total_audio_bytes_sent,
            "fallback_used": self.record.degraded_fallback_used,
        }

    def print_summary(self) -> str:
        """Produce a formatted ASCII dashboard log with latency budget compliance."""
        data = self.to_dict()
        m = data["metrics_ms"]
        b = data["budgets_ms"]
        comp = data["budget_compliance"]

        def status_badge(ok: bool) -> str:
            return "✅ OK " if ok else "⚠️ OVER"

        summary = (
            f"\n"
            f"╔════════════════════════════════════════════════════════════════════════════════╗\n"
            f"║                      LATENCY BUDGET BREAKDOWN REPORT                          ║\n"
            f"╠════════════════════════════════════════════════════════════════════════════════╣\n"
            f"║ Request ID : {self.record.request_id:<65} ║\n"
            f"║ Transcript : {self.record.transcript[:65]:<65} ║\n"
            f"║ Fallback   : {'YES (Degraded Mode)' if self.record.degraded_fallback_used else 'NO (Normal Streaming)' :<65} ║\n"
            f"╠══════════════════════════╦═══════════════╦═══════════════╦═════════════════════╣\n"
            f"║ Pipeline Stage           ║ Measured Lat  ║ Budget Target ║ Status              ║\n"
            f"╠══════════════════════════╬═══════════════╬═══════════════╬═════════════════════╣\n"
            f"║ 1. ASR Final Transcript  ║ {m['asr_latency']:>8.1f} ms  ║ {b['budget_asr']:>8.1f} ms  ║ {status_badge(comp['asr_ok']):<19} ║\n"
            f"║ 2. LLM Time-to-1st-Token ║ {m['llm_ttft']:>8.1f} ms  ║ {b['budget_llm_ttft']:>8.1f} ms  ║ {status_badge(comp['llm_ttft_ok']):<19} ║\n"
            f"║    First Clause Ready    ║ {m['first_clause_ready']:>8.1f} ms  ║        ---    ║       ---           ║\n"
            f"║ 3. TTS Time-to-1st-Byte  ║ {m['tts_ttfb']:>8.1f} ms  ║ {b['budget_tts_ttfb']:>8.1f} ms  ║ {status_badge(comp['tts_ttfb_ok']):<19} ║\n"
            f"║    Network Dispatch      ║ {m['network_buffer']:>8.1f} ms  ║        ---    ║       ---           ║\n"
            f"╠══════════════════════════╬═══════════════╬═══════════════╬═════════════════════╣\n"
            f"║ ⭐ TOTAL PERCEIVED TIME  ║ {m['perceived_turnaround']:>8.1f} ms  ║ {b['budget_perceived']:>8.1f} ms  ║ {status_badge(comp['perceived_ok']):<19} ║\n"
            f"╚══════════════════════════╩═══════════════╩═══════════════╩═════════════════════╝\n"
        )
        print(summary)
        return summary
