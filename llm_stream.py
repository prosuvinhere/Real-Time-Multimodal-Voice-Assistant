"""
Phase 1: Streaming LLM Engine with Sentence/Clause Splitting.
Yields speakable text chunks on-the-fly to minimize latency before feeding TTS.
"""

import asyncio
import json
import logging
from typing import Any, AsyncGenerator, List, Optional

try:
    import aiohttp
except ImportError:
    aiohttp = None

from chunker import SentenceChunker
from config import LLMConfig, config
from metrics import LatencyTracker

logger = logging.getLogger("llm_stream")


class StreamingLLMClient:
    """
    Streaming LLM client compatible with OpenAI, Groq, Together, and vLLM APIs.
    Integrates real-time token streaming with sentence boundary chunking.
    """

    def __init__(self, llm_config: Optional[LLMConfig] = None):
        self.cfg = llm_config or config.llm
        self.api_key = self.cfg.api_key
        self.base_url = self.cfg.base_url.rstrip("/")
        self.model = self.cfg.model
        self.system_prompt = self.cfg.system_prompt
        self.session: Optional[Any] = None

    async def init_session(self) -> None:
        if aiohttp is None:
            raise ImportError("aiohttp is required for live LLM streaming. Run: pip install aiohttp")
        if self.session is None or self.session.closed:
            self.session = aiohttp.ClientSession()

    async def stream_completion(
        self,
        user_prompt: str,
        conversation_history: Optional[List[dict]] = None,
        latency_tracker: Optional[LatencyTracker] = None,
    ) -> AsyncGenerator[str, None]:
        """
        Streams prompt to LLM and yields speakable clause chunks to TTS as soon as they form.
        """
        await self.init_session()
        chunker = SentenceChunker()

        messages = [{"role": "system", "content": self.system_prompt}]
        if conversation_history:
            messages.extend(conversation_history)
        messages.append({"role": "user", "content": user_prompt})

        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": self.model,
            "messages": messages,
            "stream": True,
            "temperature": self.cfg.temperature,
            "max_tokens": self.cfg.max_tokens,
        }

        url = f"{self.base_url}/chat/completions"

        if latency_tracker:
            latency_tracker.mark_llm_start()

        is_first_token = True
        is_first_clause = True
        full_response_text = []

        async with self.session.post(url, headers=headers, json=payload) as resp:
            if resp.status != 200:
                err_text = await resp.text()
                raise RuntimeError(f"LLM API error (status {resp.status}): {err_text}")

            async for line_bytes in resp.content:
                line = line_bytes.decode("utf-8").strip()
                if not line or not line.startswith("data:"):
                    continue

                data_str = line[len("data:"):].strip()
                if data_str == "[DONE]":
                    break

                try:
                    data = json.loads(data_str)
                    choices = data.get("choices", [])
                    if not choices:
                        continue
                    delta = choices[0].get("delta", {})
                    token = delta.get("content", "")

                    if token:
                        if is_first_token:
                            is_first_token = False
                            if latency_tracker:
                                latency_tracker.mark_llm_first_token()

                        full_response_text.append(token)

                        # Process token through clause chunker
                        new_chunks = chunker.process_token(token)
                        for clause in new_chunks:
                            if is_first_clause:
                                is_first_clause = False
                                if latency_tracker:
                                    latency_tracker.mark_first_clause_ready()
                            yield clause

                except json.JSONDecodeError:
                    continue

        # Flush any remaining text in the chunker buffer
        for remaining_clause in chunker.flush():
            if is_first_clause:
                is_first_clause = False
                if latency_tracker:
                    latency_tracker.mark_first_clause_ready()
            yield remaining_clause

        if latency_tracker:
            latency_tracker.mark_llm_complete("".join(full_response_text))

    async def close(self) -> None:
        if self.session and not self.session.closed:
            await self.session.close()


class MockLLMStream:
    """
    Offline/Simulation LLM Stream.
    Simulates realistic TTFT (~180ms) and token generation pacing (~20ms/token).
    """

    def __init__(self, simulated_ttft_ms: float = 180.0, token_delay_ms: float = 18.0):
        self.simulated_ttft = simulated_ttft_ms / 1000.0
        self.token_delay = token_delay_ms / 1000.0
        self._preset_answers = {
            "weather": "San Francisco is currently clear and 64 degrees. There is a gentle coastal breeze from the west.",
            "joke": "Why do programmers prefer dark mode? Because light attracts bugs!",
            "france": "The capital of France is Paris. It is renowned for world-class art, fashion, and the iconic Eiffel Tower.",
            "default": "I understand your question completely. Real-time voice assistants deliver seamless conversation when pipeline latency stays minimal.",
        }

    async def stream_completion(
        self,
        user_prompt: str,
        conversation_history: Optional[List[dict]] = None,
        latency_tracker: Optional[LatencyTracker] = None,
    ) -> AsyncGenerator[str, None]:
        if latency_tracker:
            latency_tracker.mark_llm_start()

        # Simulate TTFT delay (network round-trip + prefill)
        await asyncio.sleep(self.simulated_ttft)

        if latency_tracker:
            latency_tracker.mark_llm_first_token()

        # Select mock response
        prompt_lower = user_prompt.lower()
        selected_text = self._preset_answers["default"]
        for key in ["weather", "joke", "france"]:
            if key in prompt_lower:
                selected_text = self._preset_answers[key]
                break

        tokens = selected_text.split(" ")
        chunker = SentenceChunker()
        is_first_clause = True

        for i, word in enumerate(tokens):
            token = word + (" " if i < len(tokens) - 1 else "")
            await asyncio.sleep(self.token_delay)

            chunks = chunker.process_token(token)
            for c in chunks:
                if is_first_clause:
                    is_first_clause = False
                    if latency_tracker:
                        latency_tracker.mark_first_clause_ready()
                yield c

        for c in chunker.flush():
            if is_first_clause:
                is_first_clause = False
                if latency_tracker:
                    latency_tracker.mark_first_clause_ready()
            yield c

        if latency_tracker:
            latency_tracker.mark_llm_complete(selected_text)

    async def close(self) -> None:
        pass
