"""
Streaming Text Clause and Sentence Boundary Chunker.
Splits streaming LLM tokens into phonetically coherent clauses for low-latency TTS dispatch.
"""

import re
from typing import AsyncGenerator, Generator, List


class SentenceChunker:
    """
    Buffers streaming token chunks and yields speakable clauses immediately
    upon detecting punctuation boundaries, minimizing Time-To-First-Byte for TTS.
    """

    # Boundaries that indicate a natural pause or end of phrase
    SENTENCE_SPLIT_REGEX = re.compile(r'([.?!;:\n]+(?:\s+|$))')
    CLAUSE_SPLIT_REGEX = re.compile(r'([,]+(?:\s+|$))')
    MARKDOWN_STRIP_REGEX = re.compile(r'[*#_`~\[\]()]')

    def __init__(self, min_clause_chars: int = 25):
        self.min_clause_chars = min_clause_chars
        self.buffer = ""

    def clean_text(self, text: str) -> str:
        """Strip markdown and normalize whitespace that degrades TTS audio synthesis."""
        stripped = self.MARKDOWN_STRIP_REGEX.sub('', text)
        return re.sub(r'\s+', ' ', stripped).strip()

    def process_token(self, token: str) -> List[str]:
        """
        Process a new incoming token from the LLM stream.
        Returns a list of complete speakable chunks ready for TTS.
        """
        self.buffer += token
        ready_chunks = []

        # Check for primary sentence boundary
        while True:
            match = self.SENTENCE_SPLIT_REGEX.search(self.buffer)
            if match:
                end_pos = match.end()
                chunk = self.buffer[:end_pos].strip()
                self.buffer = self.buffer[end_pos:]
                cleaned = self.clean_text(chunk)
                if cleaned:
                    ready_chunks.append(cleaned)
                continue

            # If the buffer is getting long without a sentence ending, check for a comma clause boundary
            if len(self.buffer) >= self.min_clause_chars:
                comma_match = self.CLAUSE_SPLIT_REGEX.search(self.buffer)
                if comma_match:
                    end_pos = comma_match.end()
                    chunk = self.buffer[:end_pos].strip()
                    self.buffer = self.buffer[end_pos:]
                    cleaned = self.clean_text(chunk)
                    if cleaned:
                        ready_chunks.append(cleaned)
                    continue

            break

        return ready_chunks

    def flush(self) -> List[str]:
        """Flush any remaining buffered text when LLM finishes generation."""
        remainder = self.clean_text(self.buffer.strip())
        self.buffer = ""
        if remainder:
            return [remainder]
        return []
