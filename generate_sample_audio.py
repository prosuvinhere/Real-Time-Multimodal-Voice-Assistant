"""
Utility script to generate sample test WAV files for Replay Mode and Fallback Audio.
Pure Python standard library (wave, math, struct) - no external dependencies required.
"""

import math
import struct
import wave
from pathlib import Path


def create_sample_query_wav(filepath: str, duration_sec: float = 2.5, sample_rate: int = 16000) -> str:
    """
    Generates a synthetic speech-like WAV audio file (16-bit 16kHz mono PCM).
    Modulates frequencies to simulate the acoustic cadence of human speech.
    """
    path = Path(filepath)
    path.parent.mkdir(parents=True, exist_ok=True)

    num_samples = int(sample_rate * duration_sec)
    raw_frames = bytearray()

    for i in range(num_samples):
        t = float(i) / sample_rate
        # Voice pitch contour modulating between 120Hz and 220Hz
        pitch = 140.0 + 35.0 * math.sin(2.0 * math.pi * 1.5 * t)

        # Formants (F1 ~ 500Hz, F2 ~ 1500Hz)
        formant1 = math.sin(2.0 * math.pi * 500.0 * t) * 0.4
        formant2 = math.sin(2.0 * math.pi * 1500.0 * t) * 0.2
        fundamental = math.sin(2.0 * math.pi * pitch * t) * 0.7

        # Syllable modulation (intermittent pauses every ~300ms)
        syllable_envelope = max(0.0, math.sin(2.0 * math.pi * 3.5 * t))
        amplitude = 12000.0 * syllable_envelope

        sample_val = (fundamental + formant1 + formant2) * amplitude
        int_sample = max(-32768, min(32767, int(sample_val)))
        raw_frames.extend(struct.pack('<h', int_sample))

    with wave.open(str(path), 'wb') as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(sample_rate)
        wf.writeframes(raw_frames)

    print(f"Generated sample query audio: {path} ({duration_sec}s, {sample_rate}Hz 16-bit mono)")
    return str(path)


if __name__ == "__main__":
    data_dir = Path(__file__).parent / "data"
    data_dir.mkdir(exist_ok=True)
    create_sample_query_wav(str(data_dir / "sample_query.wav"), duration_sec=2.5)
