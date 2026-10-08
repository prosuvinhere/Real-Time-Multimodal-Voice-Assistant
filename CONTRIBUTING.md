# Contributing Guidelines

Thank you for your interest in contributing to the **Real-Time Multimodal Voice Assistant** project!

## Development Setup

1. **Clone the repository:**
   ```bash
   git clone https://github.com/your-username/realtime-multimodal-voice-assistant.git
   cd realtime-multimodal-voice-assistant
   ```

2. **Create and activate a virtual environment:**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate  # On Windows: .venv\Scripts\activate
   ```

3. **Install dependencies in development mode:**
   ```bash
   pip install -r requirements-dev.txt
   pip install -e .
   ```

4. **Generate sample audio assets:**
   ```bash
   python generate_sample_audio.py
   ```

## Running Tests & Benchmarks

Before opening a pull request, ensure all tests and benchmarks pass:

1. **Run the unit and integration test suite:**
   ```bash
   python run_tests.py
   ```

2. **Run the offline replay benchmark:**
   ```bash
   python replay_client.py --file data/sample_query.wav --direct
   ```

3. **Code style and linting:**
   ```bash
   ruff check .
   black --check .
   ```

## Commit Guidelines

We follow Conventional Commits:
- `feat:` A new feature or streaming capability
- `fix:` A bug fix or error handler
- `perf:` Performance improvements or latency optimizations
- `test:` Adding or refactoring test cases
- `docs:` Documentation improvements

## Submitting Pull Requests

1. Fork the repo and create your feature branch: `git checkout -b feat/ultra-low-latency-vad`.
2. Commit your changes with descriptive commit messages.
3. Push to your branch and open a Pull Request using the PR template.
4. Ensure the CI pipeline passes across all supported Python versions (3.9–3.12).
