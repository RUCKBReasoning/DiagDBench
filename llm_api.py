import logging
import os
import re
import time
from typing import Any, Dict, List, Optional, Tuple

import openai

logger = logging.getLogger(f"DB_DIAG.{__name__}")

# Conservative fallback context window used for models without an explicit entry below.
DEFAULT_CONTEXT_WINDOW_TOKENS = 131072
DEFAULT_CONTEXT_MARGIN_TOKENS = 4096
MIN_RETRY_OUTPUT_TOKENS = 8192
DEFAULT_REQUEST_RETRY_TIMES = 3
DEFAULT_REQUEST_RETRY_DELAY_SECONDS = 30.0
DEFAULT_OPENAI_TIMEOUT_SECONDS = 300.0

# Default shared output budget (reasoning + answer). A generous value avoids silent
# truncation when a model implicitly spends tokens on reasoning before emitting text.
# Overridable per request via opt['max_output_tokens'] / opt['max_tokens'].
DEFAULT_MAX_OUTPUT_TOKENS = 32768

# Optional per-model overrides. Any model not listed here falls back to the generic
# defaults above, so new models work out of the box without code changes.
MODEL_CONTEXT_WINDOWS: Dict[str, int] = {}
MODEL_MAX_OUTPUT_TOKENS: Dict[str, int] = {}

# Models whose Chat Completions endpoint rejects the legacy ``max_tokens`` parameter and
# require ``max_completion_tokens`` instead. Extend as needed; empty by default.
MAX_COMPLETION_TOKENS_MODELS: set = set()

# A complete <think>...</think> reasoning block carries no parsable payload (the answer
# lives in the <action> JSON that follows). Stripping it keeps JSON extraction robust and
# prevents the stored multi-turn context from bloating with verbose reasoning.
_THINK_BLOCK = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)


def _strip_reasoning(content):
    # Drop a closed <think>...</think> block only; leave content untouched otherwise
    # (e.g. truncated output with no closing tag) so no JSON payload is ever lost.
    if content and "<think>" in content and "</think>" in content:
        return _THINK_BLOCK.sub("", content, count=1).strip()
    return content


class LLMStreamIncompleteError(RuntimeError):
    """Raised when the provider explicitly reports an incomplete/failed stream."""


class FatalAPIQuotaError(RuntimeError):
    """Raised for permanent quota errors that must stop the entire batch."""


def _is_http_402(error: Exception) -> bool:
    status = getattr(error, "status_code", None)
    if status is None:
        status = getattr(getattr(error, "response", None), "status_code", None)
    return status == 402 or "Error code: 402" in str(error)


def resolve_api_key(opt):
    """Resolve credentials at call time; example configuration contains no secrets."""
    key = opt.get("api_key") or os.environ.get(opt.get("api_key_env", "OPENAI_API_KEY"))
    if not key:
        raise ValueError("Set the environment variable named by api_key_env.")
    return key


def build_llm_client(opt):
    """Build the single, unified OpenAI-compatible client.

    Every provider is reached through one OpenAI-compatible endpoint, so callers only
    supply ``base_url`` + an API key; no ``provider`` switch is needed anymore. Any
    OpenAI-compatible gateway (self-hosted or hosted) works by pointing ``base_url`` at it.
    """
    return openai.OpenAI(
        api_key=resolve_api_key(opt),
        base_url=opt['base_url'],
        timeout=opt.get('timeout', DEFAULT_OPENAI_TIMEOUT_SECONDS),
        **({'max_retries': opt['sdk_max_retries']} if 'sdk_max_retries' in opt else {}),
    )


def _estimate_token_count(text: str) -> int:
    """Estimate tokens without requiring provider-specific tokenizers at runtime."""
    if not text:
        return 0
    try:
        import tiktoken  # type: ignore

        encoding = tiktoken.get_encoding("cl100k_base")
        return len(encoding.encode(text))
    except Exception:
        # Mixed Chinese/English prompts are common here. char/2 is a safer fallback
        # than char/4 for CJK-heavy database schemas and SQL examples.
        return max(1, (len(text) + 1) // 2)


def _message_token_count(message: str) -> int:
    # Include a small per-message overhead for role/content wrappers.
    return _estimate_token_count(message) + 8


def _get_context_window(opt: Dict[str, Any]) -> int:
    model = opt['model_name']
    return int(opt.get('context_window_tokens') or MODEL_CONTEXT_WINDOWS.get(model, DEFAULT_CONTEXT_WINDOW_TOKENS))


def _get_max_output_tokens(opt: Dict[str, Any], default_value: Optional[int] = None) -> int:
    model = opt['model_name']
    configured = opt.get('max_output_tokens', opt.get('max_tokens'))
    if configured is not None:
        return int(configured)
    if default_value is not None:
        return int(default_value)
    return int(MODEL_MAX_OUTPUT_TOKENS.get(model, DEFAULT_MAX_OUTPUT_TOKENS))


def _backoff_output_budget(opt: Dict[str, Any]) -> int:
    current_budget = _get_max_output_tokens(opt)
    next_budget = max(MIN_RETRY_OUTPUT_TOKENS, current_budget // 2)
    opt['max_output_tokens'] = next_budget
    return next_budget


def _truncate_text_to_token_budget(text: str, token_budget: int) -> str:
    if _estimate_token_count(text) <= token_budget:
        return text

    try:
        import tiktoken  # type: ignore

        encoding = tiktoken.get_encoding("cl100k_base")
        tokens = encoding.encode(text)
        return encoding.decode(tokens[-max(1, token_budget):])
    except Exception:
        # Keep the tail: the latest instruction/action context is usually more important
        # than the oldest prefix when a single message is too large.
        char_budget = max(1, token_budget * 2)
        return text[-char_budget:]


def _trim_messages_to_context(messages: List[str], opt: Dict[str, Any]) -> Tuple[List[str], int]:
    """Keep the newest complete turns within the provider context budget."""
    context_window = _get_context_window(opt)
    max_output_tokens = _get_max_output_tokens(opt)
    margin_tokens = int(opt.get('context_margin_tokens', DEFAULT_CONTEXT_MARGIN_TOKENS))
    input_budget = context_window - max_output_tokens - margin_tokens
    if input_budget <= 0:
        # Some providers expose output limits that are separate from input limits.
        # Do not collapse the input window to zero in that case.
        input_budget = max(8192, context_window - margin_tokens)

    total_tokens = sum(_message_token_count(message) for message in messages)
    if total_tokens <= input_budget:
        return messages, total_tokens

    kept_with_index = []
    used_tokens = 0
    for index in range(len(messages) - 1, -1, -1):
        message_tokens = _message_token_count(messages[index])
        if kept_with_index and used_tokens + message_tokens > input_budget:
            break
        if not kept_with_index and message_tokens > input_budget:
            truncated = _truncate_text_to_token_budget(messages[index], max(1, input_budget - 8))
            kept_with_index.append((index, truncated))
            used_tokens = _message_token_count(truncated)
            break
        kept_with_index.append((index, messages[index]))
        used_tokens += message_tokens

    kept_with_index.reverse()
    # The legacy message format is a plain alternating list that always starts with a user
    # message. If trimming starts at an assistant turn, drop it so roles stay correct.
    while kept_with_index and kept_with_index[0][0] % 2 == 1:
        used_tokens -= _message_token_count(kept_with_index[0][1])
        kept_with_index.pop(0)

    trimmed_messages = [message for _, message in kept_with_index]
    logger.warning(
        "[LLM API][CONTEXT TRIM] model=%s original_messages=%s kept_messages=%s "
        "estimated_input_tokens=%s input_budget=%s context_window=%s max_output_tokens=%s",
        opt['model_name'],
        len(messages),
        len(trimmed_messages),
        used_tokens,
        input_budget,
        context_window,
        max_output_tokens,
    )
    return trimmed_messages, used_tokens


def _build_chat_messages(messages):
    return [{"role": "user" if i % 2 == 0 else "assistant", "content": [{"type": "text", "text": messages[i]}]} for i in range(len(messages))]


def _collect_chat_stream(stream, model: str):
    output_parts = []
    finish_reason = None
    for chunk in stream:
        if not chunk.choices:
            continue
        choice = chunk.choices[0]
        delta = getattr(choice, "delta", None)
        content = getattr(delta, "content", None)
        if content:
            output_parts.append(content)
        chunk_finish_reason = getattr(choice, "finish_reason", None)
        if chunk_finish_reason:
            finish_reason = chunk_finish_reason
    if finish_reason == "length":
        logger.warning(f"[LLM API][TRUNCATED] model={model} chat stream stopped by max_tokens.")
    return "".join(output_parts)


def _token_parameter(model: str, max_output_tokens: int, opt: Dict[str, Any]) -> Dict[str, int]:
    """Pick the output-length parameter name the model's Chat Completions API expects."""
    use_completion = (
        model in MAX_COMPLETION_TOKENS_MODELS
        or bool(opt.get('use_max_completion_tokens'))
    )
    key = "max_completion_tokens" if use_completion else "max_tokens"
    return {key: max_output_tokens}


def _call_chat(client, messages, opt):
    """Single, unified Chat Completions call path shared by every model.

    Optional per-request knobs (all passed straight through to the OpenAI-compatible
    endpoint when present in ``opt``) keep this generic without any provider branching:
      - ``reasoning_effort``: forwarded for reasoning-capable models.
      - ``extra_body``: forwarded verbatim (e.g. a provider-specific ``thinking`` switch).
    """
    model = opt['model_name']
    max_output_tokens = _get_max_output_tokens(opt)
    token_parameter = _token_parameter(model, max_output_tokens, opt)

    extra_kwargs: Dict[str, Any] = {}
    if opt.get('reasoning_effort'):
        extra_kwargs['reasoning_effort'] = opt['reasoning_effort']
    if opt.get('extra_body'):
        extra_kwargs['extra_body'] = opt['extra_body']

    stream = opt.get('stream', True)
    if stream:
        response = client.chat.completions.create(
            messages=_build_chat_messages(messages),
            model=model,
            stream=True,
            **token_parameter,
            **extra_kwargs,
        )
        return _collect_chat_stream(response, model)

    completion = client.chat.completions.create(
        messages=_build_chat_messages(messages),
        model=model,
        **token_parameter,
        **extra_kwargs,
    )
    finish_reason = getattr(completion.choices[0], "finish_reason", None)
    if finish_reason == "length":
        logger.warning(f"[LLM API][TRUNCATED] model={model} chat completion stopped by max_tokens.")
    return completion.choices[0].message.content


def get_llm_response(messages: List[str], opt):
    model_name = opt['model_name']
    client = build_llm_client(opt)

    request_opt = dict(opt)
    stream_incomplete_retries = 0
    request_retry_times = max(
        0, int(request_opt.get('request_retry_times', DEFAULT_REQUEST_RETRY_TIMES))
    )
    request_retry_delay = max(
        0.0,
        float(request_opt.get(
            'request_retry_delay_seconds', DEFAULT_REQUEST_RETRY_DELAY_SECONDS
        )),
    )
    request_failures = 0

    while True:
        try:
            request_messages, estimated_input_tokens = _trim_messages_to_context(messages, request_opt)
            start = time.time()
            content = _call_chat(client, request_messages, request_opt)
            logger.info(
                f"[LLM API Response Time] {time.time() - start} seconds "
                f"(estimated_input_tokens={estimated_input_tokens}, "
                f"max_output_tokens={_get_max_output_tokens(request_opt)})"
            )
            return _strip_reasoning(content)
        except LLMStreamIncompleteError as e:
            request_failures += 1
            stream_incomplete_retries += 1
            next_budget = _backoff_output_budget(request_opt)
            if request_failures > request_retry_times:
                raise RuntimeError(
                    f"LLM request failed after {request_failures} attempts "
                    f"(model={model_name}): {e}"
                ) from e
            if stream_incomplete_retries >= 2 and request_opt.get('stream', True):
                request_opt['stream'] = False
                logger.warning(
                    f"[LLM API][STREAM FALLBACK] model={model_name} switching to non-streaming "
                    f"after {stream_incomplete_retries} incomplete stream retries."
                )
            err_msg = " ".join(str(e).split())
            logger.error(
                f"[LLM API Error] {err_msg} --> Retrying in {request_retry_delay:g}s "
                f"({request_failures}/{request_retry_times}) "
                f"with max_output_tokens={next_budget}, stream={request_opt.get('stream', True)}..."
            )
            time.sleep(request_retry_delay)
        except Exception as e:
            if _is_http_402(e):
                raise FatalAPIQuotaError(
                    f"HTTP 402 quota error; batch must stop: {e}"
                ) from e
            request_failures += 1
            if request_failures > request_retry_times:
                raise RuntimeError(
                    f"LLM request failed after {request_failures} attempts "
                    f"(model={model_name}): {e}"
                ) from e
            err_msg = " ".join(str(e).split())
            logger.error(
                f"[LLM API Error] {err_msg} --> Retrying in {request_retry_delay:g}s "
                f"({request_failures}/{request_retry_times})..."
            )
            time.sleep(request_retry_delay)

if __name__ == "__main__":
    exit()
