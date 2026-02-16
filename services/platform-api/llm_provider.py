"""
LLM Provider abstraction for BSL agent backend.

Supports Ollama (free, local) as primary provider with graceful
fallback to no-LLM mode (direct BSL query execution).

Usage:
    provider = get_llm_provider()
    if provider.llm is not None:
        # Use BSLTools agent loop with LLM
    else:
        # Fall back to structured query builder
"""

import os
import logging
from dataclasses import dataclass
from typing import Optional

from langchain_core.language_models.chat_models import BaseChatModel

logger = logging.getLogger(__name__)


@dataclass
class LLMProviderConfig:
    """Configuration for the LLM provider."""
    # Ollama settings
    ollama_base_url: str = "http://localhost:11434"
    ollama_model: str = "qwen2.5-coder:7b"
    ollama_temperature: float = 0.0
    ollama_timeout: int = 120  # seconds — 14B can be slow on CPU

    # Google Gemini settings
    google_model: str = "gemini-2.5-pro"

    # Provider selection: "ollama", "google", "anthropic", or "none"
    provider: str = "ollama"

    @classmethod
    def from_env(cls) -> "LLMProviderConfig":
        """Load config from environment variables."""
        return cls(
            ollama_base_url=os.environ.get(
                "OLLAMA_BASE_URL", "http://localhost:11434"
            ),
            ollama_model=os.environ.get(
                "OLLAMA_MODEL", "qwen2.5-coder:7b"
            ),
            ollama_temperature=float(os.environ.get(
                "OLLAMA_TEMPERATURE", "0.0"
            )),
            ollama_timeout=int(os.environ.get(
                "OLLAMA_TIMEOUT", "120"
            )),
            google_model=os.environ.get(
                "GOOGLE_MODEL", "gemini-2.5-pro"
            ),
            provider=os.environ.get(
                "BSL_LLM_PROVIDER", "ollama"
            ),
        )


@dataclass
class LLMProvider:
    """Wraps an LLM instance with metadata about the provider."""
    llm: Optional[BaseChatModel] = None
    provider_name: str = "none"
    model_name: str = ""
    is_available: bool = False
    error_message: str = ""


def _try_ollama(config: LLMProviderConfig) -> LLMProvider:
    """Attempt to connect to Ollama."""
    try:
        from langchain_ollama import ChatOllama
        import httpx

        # Health check — hit Ollama's API to see if it's running
        health = httpx.get(
            f"{config.ollama_base_url}/api/tags",
            timeout=5.0
        )
        health.raise_for_status()

        # Check if the requested model is pulled
        models_data = health.json()
        available_models = [
            m.get("name", "") for m in models_data.get("models", [])
        ]
        # Ollama model names can have tags — match on prefix
        model_found = any(
            config.ollama_model in m for m in available_models
        )

        if not model_found:
            return LLMProvider(
                provider_name="ollama",
                model_name=config.ollama_model,
                is_available=False,
                error_message=(
                    f"Model '{config.ollama_model}' not found in Ollama. "
                    f"Available: {available_models}. "
                    f"Run: ollama pull {config.ollama_model}"
                ),
            )

        llm = ChatOllama(
            model=config.ollama_model,
            temperature=config.ollama_temperature,
            base_url=config.ollama_base_url,
            timeout=config.ollama_timeout,
        )

        return LLMProvider(
            llm=llm,
            provider_name="ollama",
            model_name=config.ollama_model,
            is_available=True,
        )

    except ImportError:
        return LLMProvider(
            provider_name="ollama",
            error_message="langchain-ollama not installed",
        )
    except Exception as e:
        return LLMProvider(
            provider_name="ollama",
            model_name=config.ollama_model,
            error_message=f"Ollama unavailable: {e}",
        )


def _try_google(config: LLMProviderConfig) -> LLMProvider:
    """Attempt to use Google Gemini (free via Google AI Studio)."""
    try:
        api_key = os.environ.get("GOOGLE_API_KEY")
        if not api_key:
            return LLMProvider(
                provider_name="google",
                error_message="GOOGLE_API_KEY not set",
            )

        from langchain_google_genai import ChatGoogleGenerativeAI

        llm = ChatGoogleGenerativeAI(
            model=config.google_model,
            temperature=0,
            google_api_key=api_key,
        )

        return LLMProvider(
            llm=llm,
            provider_name="google",
            model_name=config.google_model,
            is_available=True,
        )

    except ImportError:
        return LLMProvider(
            provider_name="google",
            error_message="langchain-google-genai not installed",
        )
    except Exception as e:
        return LLMProvider(
            provider_name="google",
            error_message=f"Google Gemini unavailable: {e}",
        )


def _try_anthropic(config: LLMProviderConfig) -> LLMProvider:
    """Attempt to use Anthropic Claude (paid fallback)."""
    try:
        api_key = os.environ.get("ANTHROPIC_API_KEY")
        if not api_key:
            return LLMProvider(
                provider_name="anthropic",
                error_message="ANTHROPIC_API_KEY not set",
            )

        from langchain_anthropic import ChatAnthropic

        llm = ChatAnthropic(
            model="claude-sonnet-4-20250514",
            temperature=0,
            api_key=api_key,
        )

        return LLMProvider(
            llm=llm,
            provider_name="anthropic",
            model_name="claude-sonnet-4-20250514",
            is_available=True,
        )

    except ImportError:
        return LLMProvider(
            provider_name="anthropic",
            error_message="langchain-anthropic not installed",
        )
    except Exception as e:
        return LLMProvider(
            provider_name="anthropic",
            error_message=f"Anthropic unavailable: {e}",
        )


# Provider resolution chain
_PROVIDER_CHAIN = {
    "ollama": [_try_ollama],
    "google": [_try_google],
    "anthropic": [_try_anthropic],
    "auto": [_try_ollama, _try_google, _try_anthropic],
    "none": [],
}

# Cached singleton
_cached_provider: Optional[LLMProvider] = None


def get_llm_provider(force_refresh: bool = False) -> LLMProvider:
    """
    Get the configured LLM provider with fallback chain.

    Resolution order based on BSL_LLM_PROVIDER env var:
      "ollama"    -> Ollama only (default)
      "anthropic" -> Anthropic only
      "auto"      -> Ollama -> Anthropic -> none
      "none"      -> No LLM (structured query builder only)

    Returns LLMProvider with llm=None if no provider is available.
    """
    global _cached_provider
    if _cached_provider is not None and not force_refresh:
        return _cached_provider

    config = LLMProviderConfig.from_env()
    resolvers = _PROVIDER_CHAIN.get(config.provider, [])

    for resolver in resolvers:
        provider = resolver(config)
        if provider.is_available:
            logger.info(
                f"[BSL] LLM provider ready: {provider.provider_name} "
                f"({provider.model_name})"
            )
            _cached_provider = provider
            return provider
        else:
            logger.warning(
                f"[BSL] Provider {provider.provider_name} unavailable: "
                f"{provider.error_message}"
            )

    # No provider available — return empty provider
    fallback = LLMProvider(
        provider_name="none",
        error_message="No LLM provider available. Using structured query builder.",
    )
    logger.info(f"[BSL] {fallback.error_message}")
    _cached_provider = fallback
    return fallback
