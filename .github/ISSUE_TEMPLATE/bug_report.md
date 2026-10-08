---
name: Bug Report
about: Create a report to help us improve
title: '[BUG] '
labels: bug
assignees: ''
---

**Describe the Bug**
A clear and concise description of what the bug is.

**Pipeline Stage Affected**
- [ ] Phase 1: Streaming Pipeline (ASR / LLM / TTS / WebSocket)
- [ ] Phase 2: Latency Budget & Tracking (Metrics / Timers)
- [ ] Phase 3: Resilience (Timeouts / Fallback Audio / Replay Mode)

**Environment Details**
- OS: [e.g. macOS 14.4, Ubuntu 22.04]
- Python Version: [e.g. 3.10.12]
- Audio Provider: [Deepgram, ElevenLabs, Cartesia, or Mock]

**Steps to Reproduce**
1. Run `python server.py`
2. Run `python replay_client.py --file ...`
3. See error

**Expected Behavior**
A clear description of what you expected to happen.

**Screenshots / Logs / Latency Output**
Paste terminal logs or the latency report table if applicable.
