"""
Configuration and Environment Settings for Real-Time Multimodal Voice Assistant.
"""

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass
class LatencyBudgets:
    """Latency budget thresholds in milliseconds."""
    asr_ms: float = 350.0                # User speech stop -> ASR final transcript
    llm_ttft_ms: float = 250.0           # ASR transcript -> LLM First Token
    first_clause_ms: float = 350.0       # ASR transcript -> First speech-ready clause
    tts_ttfb_ms: float = 150.0           # First clause -> TTS First Audio Byte
    total_perceived_ms: float = 800.0    # User speech stop -> Audio arrives at client


@dataclass
class TimeoutConfig:
    """Strict timeout and resilience configuration."""
    asr_silence_sec: float = 5.0         # Max idle silence before closing ASR session
    llm_ttft_sec: float = 1.5            # Abort LLM if first token exceeds this budget
    tts_ttfb_sec: float = 1.0            # Abort TTS if first audio byte exceeds this budget
    circuit_breaker_threshold: int = 3   # Consecutive upstream failures before tripping


@dataclass
class ASRConfig:
    """ASR (Deepgram) configuration."""
    api_key: str = field(default_factory=lambda: os.getenv("DEEPGRAM_API_KEY", ""))
    model: str = os.getenv("DEEPGRAM_MODEL", "nova-2")
    sample_rate: int = int(os.getenv("DEEPGRAM_SAMPLE_RATE", "16000"))
    channels: int = 1
    encoding: str = "linear16"
    endpointing_ms: int = int(os.getenv("DEEPGRAM_ENDPOINTING_MS", "300"))


@dataclass
class LLMConfig:
    """LLM configuration (OpenAI/Groq compatible streaming)."""
    api_key: str = field(default_factory=lambda: os.getenv("OPENAI_API_KEY", ""))
    base_url: str = os.getenv("LLM_BASE_URL", "https://api.openai.com/v1")
    model: str = os.getenv("LLM_MODEL", "gpt-4o-mini")
    system_prompt: str = os.getenv(
        "LLM_SYSTEM_PROMPT",
        "You are a helpful, conversational, lightning-fast voice assistant. "
        "Respond concisely in 1-2 natural spoken sentences. Avoid markdown, lists, and emojis."
    )
    temperature: float = 0.6
    max_tokens: int = 150


@dataclass
class TTSConfig:
    """TTS (ElevenLabs / Cartesia) configuration."""
    provider: str = os.getenv("TTS_PROVIDER", "elevenlabs")  # 'elevenlabs' or 'cartesia'
    elevenlabs_api_key: str = field(default_factory=lambda: os.getenv("ELEVENLABS_API_KEY", ""))
    elevenlabs_voice_id: str = os.getenv("ELEVENLABS_VOICE_ID", "21m00Tcm4TlvDq8ikWAM")
    elevenlabs_model_id: str = os.getenv("ELEVENLABS_MODEL_ID", "eleven_turbo_v2_5")
    elevenlabs_output_format: str = os.getenv("ELEVENLABS_OUTPUT_FORMAT", "pcm_16000")
    
    cartesia_api_key: str = field(default_factory=lambda: os.getenv("CARTESIA_API_KEY", ""))
    cartesia_voice_id: str = os.getenv("CARTESIA_VOICE_ID", "")


@dataclass
class AppConfig:
    """Global Application Configuration."""
    host: str = os.getenv("HOST", "0.0.0.0")
    port: int = int(os.getenv("PORT", "8000"))
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    mock_apis: bool = os.getenv("MOCK_APIS", "true").lower() in ("true", "1", "yes")
    
    fallback_audio_file: str = os.getenv(
        "FALLBACK_AUDIO_FILE",
        str(Path(__file__).parent / "static" / "fallback_trouble_connecting.wav")
    )
    
    budgets: LatencyBudgets = field(default_factory=LatencyBudgets)
    timeouts: TimeoutConfig = field(default_factory=TimeoutConfig)
    asr: ASRConfig = field(default_factory=ASRConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    tts: TTSConfig = field(default_factory=TTSConfig)


# Global configuration instance
config = AppConfig()
