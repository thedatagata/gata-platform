"""
Ollama Setup Script for GATA Platform.

Checks if Ollama is installed and running, pulls the required model,
and validates the BSL agent integration.

Usage:
    python scripts/setup_ollama.py              # Check status
    python scripts/setup_ollama.py --install     # Install instructions
    python scripts/setup_ollama.py --pull        # Pull the model
    python scripts/setup_ollama.py --test        # Run integration test
"""

import sys
import subprocess
import argparse
from pathlib import Path

try:
    import httpx
    HAS_HTTPX = True
except ImportError:
    HAS_HTTPX = False


OLLAMA_BASE_URL = "http://localhost:11434"
DEFAULT_MODEL = "qwen2.5-coder:14b"


def check_ollama_running() -> bool:
    """Check if Ollama server is running."""
    if not HAS_HTTPX:
        print("[WARN] httpx not installed, using subprocess fallback")
        try:
            result = subprocess.run(
                ["ollama", "list"], capture_output=True, text=True, timeout=10
            )
            return result.returncode == 0
        except (FileNotFoundError, subprocess.TimeoutExpired):
            return False

    try:
        resp = httpx.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5.0)
        return resp.status_code == 200
    except Exception:
        return False


def get_available_models() -> list[str]:
    """Get list of models available in Ollama."""
    if not HAS_HTTPX:
        try:
            result = subprocess.run(
                ["ollama", "list"], capture_output=True, text=True, timeout=10
            )
            if result.returncode == 0:
                lines = result.stdout.strip().split("\n")[1:]  # skip header
                return [line.split()[0] for line in lines if line.strip()]
        except Exception:
            pass
        return []

    try:
        resp = httpx.get(f"{OLLAMA_BASE_URL}/api/tags", timeout=5.0)
        data = resp.json()
        return [m["name"] for m in data.get("models", [])]
    except Exception:
        return []


def pull_model(model: str) -> bool:
    """Pull a model via Ollama CLI."""
    print(f"[PULL] Pulling {model}... This may take several minutes.")
    try:
        result = subprocess.run(
            ["ollama", "pull", model],
            timeout=1800,  # 30 min timeout for large models
        )
        return result.returncode == 0
    except FileNotFoundError:
        print("[ERROR] 'ollama' command not found. Install Ollama first.")
        return False
    except subprocess.TimeoutExpired:
        print("[ERROR] Pull timed out after 30 minutes.")
        return False


def run_integration_test(model: str) -> bool:
    """Test the full BSL agent flow."""
    print("\n[TEST] Running BSL agent integration test...")

    if not HAS_HTTPX:
        print("[ERROR] httpx required for integration test")
        return False

    # Test 1: LLM provider
    print("  1. Testing LLM provider...")
    try:
        sys.path.insert(0, str(Path(__file__).parent.parent / "services" / "platform-api"))
        from llm_provider import get_llm_provider

        provider = get_llm_provider(force_refresh=True)
        if not provider.is_available:
            print(f"  [FAIL] Provider unavailable: {provider.error_message}")
            return False
        print(f"  [PASS] Provider: {provider.provider_name} ({provider.model_name})")
    except Exception as e:
        print(f"  [FAIL] Import error: {e}")
        return False

    # Test 2: BSL semantic tables
    print("  2. Testing BSL semantic table builder...")
    try:
        from bsl_model_builder import build_tenant_semantic_tables
        tables = build_tenant_semantic_tables("tyrell_corp")
        if not tables:
            print("  [FAIL] No semantic tables built")
            return False
        print(f"  [PASS] Built {len(tables)} semantic tables")
    except Exception as e:
        print(f"  [FAIL] Build error: {e}")
        return False

    # Test 3: Agent ask (with LLM)
    print("  3. Testing BSL agent ask...")
    try:
        from bsl_agent import ask
        result = ask("What are the top campaigns by spend?", "tyrell_corp")
        print(f"  [PASS] Provider: {result.provider}")
        print(f"  [PASS] Answer: {result.answer[:100]}...")
        print(f"  [PASS] Records: {len(result.records)}")
        print(f"  [PASS] Execution: {result.execution_time_ms}ms")
    except Exception as e:
        print(f"  [FAIL] Agent error: {e}")
        return False

    print("\n[SUCCESS] All integration tests passed!")
    return True


def print_install_instructions():
    """Print Ollama installation instructions."""
    print("""
=== Ollama Installation Guide ===

Windows:
  1. Download from https://ollama.com/download/windows
  2. Run the installer
  3. Ollama starts automatically as a system service

macOS:
  brew install ollama
  brew services start ollama

Linux:
  curl -fsSL https://ollama.com/install.sh | sh

After installing:
  ollama pull qwen2.5-coder:14b

Hardware requirements for qwen2.5-coder:14b:
  - GPU: ~10GB VRAM (RTX 3060 12GB or better)
  - CPU-only: ~16GB RAM (slower, but works)
  - Disk: ~9GB for model weights

Smaller alternatives:
  ollama pull qwen2.5-coder:7b    # ~4.5GB, needs ~6GB VRAM
  ollama pull qwen2.5-coder:3b    # ~2GB, needs ~4GB VRAM
""")


def main():
    parser = argparse.ArgumentParser(description="Ollama setup for GATA Platform")
    parser.add_argument("--install", action="store_true", help="Show install instructions")
    parser.add_argument("--pull", action="store_true", help="Pull the default model")
    parser.add_argument("--model", default=DEFAULT_MODEL, help=f"Model to use (default: {DEFAULT_MODEL})")
    parser.add_argument("--test", action="store_true", help="Run integration test")
    args = parser.parse_args()

    if args.install:
        print_install_instructions()
        return

    # Status check
    print("=== Ollama Status ===")
    running = check_ollama_running()
    print(f"  Server: {'Running' if running else 'Not running'}")

    if running:
        models = get_available_models()
        print(f"  Models: {', '.join(models) if models else 'none'}")

        has_model = any(args.model in m for m in models)
        print(f"  {args.model}: {'Available' if has_model else 'Not pulled'}")
    else:
        print("  Run 'ollama serve' to start the server")
        print("  Run 'python scripts/setup_ollama.py --install' for setup guide")

    if args.pull:
        if not running:
            print("\n[ERROR] Ollama not running. Start it first: ollama serve")
            sys.exit(1)
        success = pull_model(args.model)
        if not success:
            sys.exit(1)

    if args.test:
        if not running:
            print("\n[ERROR] Ollama not running. Start it first.")
            sys.exit(1)
        success = run_integration_test(args.model)
        if not success:
            sys.exit(1)


if __name__ == "__main__":
    main()
