"""
Phase 3: Fallback Audio Generator & Streaming Provider.
Ensures zero-silence graceful degradation when upstream APIs (ASR/LLM/TTS) fail or hang.
Synthesizes and caches fallback PCM audio without requiring third-party tools.
"""

import asyncio
import io
import math
import os
import struct
import wave
from pathlib import Path
from typing import AsyncGenerator, Optional

from config import config


def generate_synthesized_chime_wav(filename: str, duration_sec: float = 2.0, sample_rate: int = 16000) -> str:
    """
    Synthesize a clean auditory chime/prompt WAV file in standard 16-bit 16kHz mono PCM.
    Acts as a guaranteed fallback audio asset if no external recording is present.
    """
    path = Path(filename)
    path.parent.mkdir(parents=True, exist_ok=True)

    num_samples = int(sample_rate * duration_sec)
    
    # Generate multi-frequency warm notification chord (C-maj: 523.25Hz, 659.25Hz, 783.99Hz)
    frequencies = [523.25, 659.25, 783.99]
    amplitude = 10000.0  # Safe amplitude for 16-bit signed PCM (max 32767)

    raw_frames = bytearray()
    for i in range(num_samples):
        t = float(i) / sample_rate
        # Exponential decay envelope for gentle audio chime
        envelope = math.exp(-2.5 * (t % 1.0))
        
        sample_val = 0.0
        for freq in frequencies:
            sample_val += math.sin(2.0 * math.pi * freq * t)
        
        sample_val = (sample_val / len(frequencies)) * amplitude * envelope
        int_sample = max(-32768, min(32767, int(sample_val)))
        raw_frames.extend(struct.pack('<h', int_sample))

    with wave.open(str(path), 'wb') as wav_file:
        wav_file.setnchannels(1)        # Mono
        wav_file.setsampwidth(2)       # 16-bit
        wav_file.setframerate(sample_rate)
        wav_file.writeframes(raw_frames)

    return str(path)


class FallbackAudioProvider:
    """Provides streaming fallback audio chunks during system failure."""

    def __init__(self, audio_file_path: Optional[str] = None):
        self.audio_file_path = audio_file_path or config.fallback_audio_file
        self._ensure_fallback_exists()
        self._cached_pcm_data = self._load_pcm_data()

    def _ensure_fallback_exists(self) -> None:
        if not os.path.exists(self.audio_file_path):
            generate_synthesized_chime_wav(self.audio_file_path)

    def _load_pcm_data(self) -> bytes:
        """Read raw PCM frames from the WAV file."""
        try:
            with wave.open(self.audio_file_path, 'rb') as wf:
                return wf.readframes(wf.getnframes())
        except Exception:
            # Fallback memory buffer
            return b'\x00' * 3200

    async def stream_audio_chunks(
        self,
        chunk_size: int = 1600,      # 1600 bytes = 50ms at 16kHz 16-bit mono
        delay_sec: float = 0.05
    ) -> AsyncGenerator[bytes, None]:
        """
        Stream pre-recorded fallback audio to client in paced real-time chunks.
        Simulates live TTS delivery while the client receives failure audio.
        """
        offset = 0
        total_len = len(self._cached_pcm_data)
        while offset < total_len:
            end = min(offset + chunk_size, total_len)
            chunk = self._cached_pcm_data[offset:end]
            offset = end
            yield chunk
            await asyncio.sleep(delay_sec)


# Global fallback audio provider instance
fallback_provider = FallbackAudioProvider()
