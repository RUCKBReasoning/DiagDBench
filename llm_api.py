import copy
import math
from functools import lru_cache
import logging
import json
import os
import re
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from urllib.parse import urlsplit, urlunsplit

# Context budgeting and stdlib-based native adapters do not require the SDK.
# Require it only when an API client is actually constructed.
try:
    import openai
except ModuleNotFoundError as exc:
    if exc.name != "openai":
        raise
    openai = None

# Shared, SDK-independent context budgeting; stored histories are never edited.
LOGGER = logging.getLogger('DB_DIAG.llm_api.context')
VERSION = '2026-10-25-context-v1'
MARKER = '\n[CONTEXT TRUNCATED: omitted data is unknown, not absent.]\n'
CONTEXT_FIELDS = frozenset({
    'context_window_tokens', 'max_input_tokens', 'model_max_output_tokens',
    'context_margin_tokens', 'minimum_input_tokens', 'tokenizer_encoding',
    'tokenizer_estimate_factor', 'context_limit_source', 'allow_context_truncation',
})


class ContextBudgetError(ValueError):
    """Invalid limits or immutable instructions/tools alone exceed the budget."""


def _positive(value, name, allow_zero=False):
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
        raise ContextBudgetError(name + ' must be a finite integer')
    if int(value) != value or value < (0 if allow_zero else 1):
        raise ContextBudgetError(name + ' is out of range')
    return int(value)


@lru_cache(maxsize=8)
def _registry(path, mtime):
    try:
        data = json.loads(Path(path).read_text())
        return {str(v['model_name']): {k: x for k, x in v.items() if k in CONTEXT_FIELDS}
                for v in data.values() if isinstance(v, dict) and v.get('model_name')}
    except (OSError, ValueError, AttributeError):
        return {}


def settings(opt):
    path = Path(__file__).resolve().parent / 'Configs/llm_config.json'
    try:
        defaults = _registry(str(path), path.stat().st_mtime_ns).get(str(opt.get('model_name', opt.get('model', ''))), {})
    except OSError:
        defaults = {}
    return dict(defaults, **opt)


@lru_cache(maxsize=16)
def _encoding(name):
    try:
        import tiktoken
        return tiktoken.get_encoding(name)
    except Exception:
        # No download, model substitution, or dependency installation at request time.
        return None


def count_tokens(text, opt=None):
    opt = opt or {}
    enc = _encoding(opt.get('tokenizer_encoding', 'cl100k_base'))
    if enc is None:
        # Conservative bound for byte-level tokenizers; unlike char/2 this handles CJK.
        return len(text.encode('utf-8'))
    factor = float(opt.get('tokenizer_estimate_factor', 1.15))
    if not math.isfinite(factor) or factor < 1:
        raise ContextBudgetError('tokenizer_estimate_factor must be finite and >= 1')
    return math.ceil(len(enc.encode(text, disallowed_special=())) * factor)


def resolve_limits(opt, requested_output=None):
    opt = settings(opt)
    window = _positive(opt.get('context_window_tokens', 131072), 'context_window_tokens')
    margin = _positive(opt.get('context_margin_tokens', 4096), 'context_margin_tokens', True)
    reserve = _positive(opt.get('minimum_input_tokens', min(32768, max(1, (window-margin)//4))), 'minimum_input_tokens')
    if margin + reserve >= window:
        raise ContextBudgetError('context margin and minimum input leave no output budget')
    requested = _positive(requested_output if requested_output is not None else opt.get('max_output_tokens', opt.get('max_tokens', 32768)), 'max_output_tokens')
    output = min(requested, _positive(opt.get('model_max_output_tokens', requested), 'model_max_output_tokens'), window-margin-reserve)
    budget = min(_positive(opt.get('max_input_tokens', window), 'max_input_tokens'), window-margin-output)
    return opt, {'version': VERSION, 'model': opt.get('model_name', opt.get('model', 'unknown')),
                 'context_window': window, 'input_budget': budget, 'output_budget': output,
                 'requested_output_budget': requested, 'margin': margin,
                 'limit_source': opt.get('context_limit_source', 'conservative_fallback_not_provider_max'),
                 'token_count_mode': 'estimated_tokenizer' if _encoding(opt.get('tokenizer_encoding', 'cl100k_base')) else 'utf8_byte_upper_bound',
                 'output_budget_adjusted': output != requested}


def truncate_text(text, budget, opt=None):
    """Head+tail truncation on Unicode boundaries; always verify the final count."""
    opt = opt or {}
    if count_tokens(text, opt) <= budget:
        return text
    if budget <= 0:
        return ''
    marker = MARKER if count_tokens(MARKER, opt) <= budget else ''
    lo, hi = 0, len(text)
    best = marker
    while lo <= hi:
        n = (lo+hi)//2
        head = (n*3)//4
        tail = n-head
        candidate = text[:head] + marker + (text[-tail:] if tail else '')
        if count_tokens(candidate, opt) <= budget:
            best = candidate
            lo = n+1
        else:
            hi = n-1
    assert count_tokens(best, opt) <= budget
    return best


def _dump(value):
    return json.dumps(value, ensure_ascii=False, separators=(',', ':'))


def request_tokens(request, opt):
    # Count the actual input envelope, including system prompts, tools and response schemas.
    value = {k: v for k, v in request.items() if k in ('messages','input','prompt','instructions','tools','tool_choice','response_format','text')}
    return count_tokens(_dump(value), opt) + 16


def _text_slots(messages):
    for index, msg in enumerate(messages):
        if msg.get('role') in ('system', 'developer'):
            continue
        value = msg.get('content')
        if isinstance(value, str):
            yield index, msg, 'content', value
        elif isinstance(value, list):
            for part in value:
                if isinstance(part, dict) and isinstance(part.get('text'), str):
                    yield index, part, 'text', part['text']


def _history_groups(messages):
    groups = []
    for i, m in enumerate(messages):
        if m.get('role') in ('system', 'developer'):
            continue
        if not groups or m.get('role') == 'user':
            groups.append([])
        groups[-1].append(i)
    return groups


def _validate_tools(messages):
    pending = set()
    for message in messages:
        role = message.get('role')
        if role == 'tool':
            call_id = message.get('tool_call_id')
            if call_id not in pending:
                raise ContextBudgetError('Orphan tool result in model request')
            pending.remove(call_id)
        else:
            if pending:
                raise ContextBudgetError('Incomplete tool result group in model request')
            pending.update(c['id'] for c in message.get('tool_calls', []))
    if pending:
        raise ContextBudgetError('Unresolved tool calls in model request')


def prepare_request(request, opt=None):
    """Fit a structured Chat request without mutating the caller's history/options."""
    request = copy.deepcopy(request)
    raw_opt = dict(opt or {})
    raw_opt.setdefault('model_name', request.get('model', 'unknown'))
    output_key = next((k for k in ('max_completion_tokens','max_output_tokens','max_tokens') if k in request), 'max_tokens')
    cfg, meta = resolve_limits(raw_opt, request.get(output_key))
    request[output_key] = meta['output_budget']
    before = request_tokens(request, cfg)
    meta.update(original_input_tokens=before, input_tokens=before, truncated=False, dropped_messages=0)
    messages = request.get('messages')
    if not isinstance(messages, list):
        raise ContextBudgetError('Structured request requires messages list')
    _validate_tools(messages)
    budget = meta['input_budget']
    if before > budget and cfg.get('allow_context_truncation', True) is False:
        raise ContextBudgetError('Input exceeds configured budget with truncation disabled')
    # Preserve the first task and the newest turn. Remove whole older user/assistant/tool groups.
    while request_tokens(request, cfg) > budget:
        groups = _history_groups(messages)
        if len(groups) <= 2:
            break
        remove = set(groups[1])
        meta['dropped_messages'] += len(remove)
        messages[:] = [v for i,v in enumerate(messages) if i not in remove]
    # Long observations first; never truncate tool-call IDs/arguments or system instructions.
    for _ in range(128):
        current = request_tokens(request, cfg)
        if current <= budget:
            break
        slots = [slot for slot in _text_slots(messages) if slot[3]]
        if not slots:
            raise ContextBudgetError('Immutable system/tool schema exceeds input budget')
        slots.sort(key=lambda v: (messages[v[0]].get('role') == 'tool', count_tokens(v[3], cfg)), reverse=True)
        _, owner, key, text = slots[0]
        old_n = count_tokens(text, cfg)
        if not old_n:
            raise ContextBudgetError('Immutable system/tool arguments exceed input budget')
        target = max(0, old_n-max(current-budget+64, old_n//8, 1))
        owner[key] = truncate_text(text, target, cfg)
    after = request_tokens(request, cfg)
    if after > budget:
        raise ContextBudgetError('Request still exceeds input budget after truncation')
    _validate_tools(messages)
    meta.update(input_tokens=after, truncated=before != after)
    if meta['truncated'] or meta['output_budget_adjusted']:
        LOGGER.warning('[CONTEXT BUDGET] %s', _dump(meta))
    return request, meta


def prepare_legacy(messages, opt):
    req = {'model':opt['model_name'], 'max_tokens':opt.get('max_output_tokens', opt.get('max_tokens',32768)),
           'messages':[{'role':'user' if i%2==0 else 'assistant','content':v} for i,v in enumerate(messages)]}
    request, meta = prepare_request(req, opt)
    roles = [m['role'] for m in request['messages']]
    if any(role != ('user' if i%2==0 else 'assistant') for i,role in enumerate(roles)):
        raise ContextBudgetError('Truncation changed legacy alternating role order')
    return [m['content'] for m in request['messages']], meta


def prepare_judge_prompt(prompt, opt, kind):
    """Protect the complete rubric; truncate evidence before SQL, with explicit provenance."""
    cfg, meta = resolve_limits(opt)
    def tokens(text):
        return request_tokens({'messages':[{'role':'user','content':text}]}, cfg)
    before = tokens(prompt)
    meta.update(original_input_tokens=before, input_tokens=before, truncated=False, sql_truncated=False,
                evidence_truncated=False, input_complete=True, kind=kind)
    if before <= meta['input_budget'] - 256:
        return prompt, meta
    budget = meta['input_budget'] - 256
    notice = ('\nCONTEXT LIMIT NOTICE: Some quoted data below has been truncated, not omitted by the candidate. '
              'Missing data is unknown. Do not invent it or claim complete evidence was inspected. '
              'Use retained evidence and explicitly state material uncertainty.\n')
    if kind == 'workload':
        head, raw = prompt.split('INPUT DATA:\n',1)
        data = json.loads(raw)
        # The previous lossless-encoding note must not claim complete evidence after truncation.
        head = head.replace('Nothing is omitted.', 'Context truncation, if present, is explicitly marked.')
        original_evidence = data.get('execution')
        full_evidence = _dump(original_evidence)
        def render():return head+notice+'INPUT DATA:\n'+_dump(data)
        lo, hi = 0, count_tokens(full_evidence,cfg);best = None
        while lo <= hi:
            cap = (lo+hi)//2
            data['execution'] = {'_context_truncated':True,'preview':truncate_text(full_evidence,cap,cfg)}
            candidate = render()
            if tokens(candidate) <= budget:
                best = candidate;lo=cap+1
            else:hi=cap-1
        meta['evidence_truncated']=True
        if best is None:
            data['execution']={'_context_truncated':True,'notice':'Execution evidence omitted to fit context.'}
            fields=['original_verified_workload','optimized_verified_workload']
            originals={k:data[k] for k in fields}
            total=sum(count_tokens(v,cfg) for v in originals.values()) or 1
            lo,hi=0,total
            while lo<=hi:
                cap=(lo+hi)//2
                for key,value in originals.items():
                    data[key]=truncate_text(value,max(0,int(cap*count_tokens(value,cfg)/total)),cfg)
                candidate=render()
                if tokens(candidate)<=budget:best=candidate;lo=cap+1
                else:hi=cap-1
            meta['sql_truncated']=True
        if best is None:raise ContextBudgetError('Judge rubric alone exceeds budget')
        result=best
    elif kind == 'diagnosis':
        parts=re.split(r'(\n===== [^\n]+ =====\n)', '\n'+prompt)
        head=parts[0].lstrip('\n')+notice
        sections=[(parts[i],parts[i+1]) for i in range(1,len(parts)-1,2)]
        if not sections:raise ContextBudgetError('Cannot locate diagnosis Judge sections')
        costs=[count_tokens(v,cfg) for _,v in sections]
        lo,hi=0,max(costs);best=None
        while lo<=hi:
            cap=(lo+hi)//2
            candidate=head+''.join(label+truncate_text(value,cap*(3 if 'CANDIDATE' in label or 'GROUND TRUTH' in label else 1),cfg) for label,value in sections)
            if tokens(candidate)<=budget:best=candidate;lo=cap+1
            else:hi=cap-1
        if best is None:raise ContextBudgetError('Diagnosis rubric alone exceeds budget')
        result=best;meta['evidence_truncated']=True
        # Report references may include SQL; do not describe this as complete evidence.
        meta['sql_truncated']=any('.sql' in label and value not in result for label,value in sections)
    else:
        raise ContextBudgetError('Unknown Judge prompt kind')
    meta.update(input_tokens=tokens(result),truncated=True,input_complete=False)
    assert meta['input_tokens']<=budget
    LOGGER.warning('[JUDGE CONTEXT BUDGET] %s',_dump(meta))
    return result,meta


# Provider clients, transport and retry handling.
logger = logging.getLogger(f"DB_DIAG.{__name__}")

# Implicit reasoning effort for OpenAI-family reasoning models.
DEFAULT_REASONING_EFFORT = "medium"

# Conservative fallback context window used for models without an explicit entry below.
DEFAULT_CONTEXT_WINDOW_TOKENS = 131072
DEFAULT_CONTEXT_MARGIN_TOKENS = 4096
MIN_RETRY_OUTPUT_TOKENS = 8192
DEFAULT_REQUEST_RETRY_TIMES = 3
DEFAULT_REQUEST_RETRY_DELAY_SECONDS = 30.0
DEFAULT_AISKT_TIMEOUT_SECONDS = 300.0

# Responses endpoint applies a tiny default output cap when max_output_tokens is omitted;
# implicit reasoning tokens exhaust it before any visible text, yielding an empty output.
# Set a generous budget (shared by reasoning + answer) to avoid truncation. This bounds the
# OpenAI-family reasoning models only; Doubao has its own much larger budget below.
RESPONSES_MAX_OUTPUT_TOKENS = 32768

# Doubao (Volces Ark) deep thinking is enabled so the model can reason fully before emitting
# the <action> JSON. Only "enabled" / "disabled" are accepted (not "auto").
DOUBAO_THINKING_TYPE = "enabled"

# With thinking enabled the chain-of-thought and the answer share one output budget. Ark's
# model list documents Seed 2.1 Pro/Turbo as 256k context and 256k max answer, so give the full
# ceiling while the retry path below can back off if a provider-side stream fails.
DOUBAO_MAX_OUTPUT_TOKENS = 262144

# GLM (Zhipu AI) on Volces Ark reuses the Ark-specific "thinking" switch. GLM-5.2 already
# defaults to deep thinking, but we pass it explicitly so reasoning mode stays deterministic
# regardless of any account-level default. Only "enabled" / "disabled" are accepted.
GLM_THINKING_TYPE = "enabled"
GLM_MAX_OUTPUT_TOKENS = 131072

# DeepSeek-V4 (Volces Ark) uses the Responses API with the built-in web_search tool for
# real-time info. max_keyword caps the keywords per search round (Ark range 1-50, 1-10
# recommended) to bound cost; overridable via opt['web_search_max_keyword'].
DEEPSEEK_WEB_SEARCH_MAX_KEYWORD = 2
# DeepSeek-V4-Pro officially advertises a 1M context window and up to 384k output. Very large
# output caps can still make streaming fragile under provider load, so repeated
# LLMStreamIncompleteError retries dynamically back off this per-request value.
DEEPSEEK_MAX_OUTPUT_TOKENS = 393216

# A complete <think>...</think> reasoning block carries no parsable payload (the answer
# lives in the <action> JSON that follows). Stripping it keeps JSON extraction robust and
# prevents the stored multi-turn context from bloating with verbose reasoning.
_THINK_BLOCK = re.compile(r"<think>.*?</think>", re.DOTALL | re.IGNORECASE)


def _strip_reasoning(content):
    # Drop a closed <think>...</think> block only; leave content untouched otherwise
    # (e.g. truncated output with no closing tag) so no JSON payload is ever lost.
    if content and "<think>" in content and "</think>" in content:
        text = _THINK_BLOCK.sub("", content, count=1).strip()
        if getattr(content, "truncated", False):
            return LLMResponseText(text, finish_reason=content.finish_reason)
        return text
    return content

DOUBAO_MODELS = {
    "doubao-seed-2-1-pro-260628",
    "doubao-seed-2-1-turbo-260628",
}

# OpenAI-family reasoning models, dispatched with a reasoning-effort knob.
OPENAI_REASONING_MODELS = {
    "gpt-5.5-2026-04-24",
    "gpt-5.3-codex-2026-02-24",
    "gpt-5.2-codex-2026-01-14",
    "o3-2025-04-16",
}

# These Chat Completions models reject the legacy max_tokens parameter.
MAX_COMPLETION_TOKENS_MODELS = {
    "GPT-6.1-Sol",
}

# GLM (Zhipu AI) models served through the Volces Ark OpenAI-compatible endpoint. They use the
# chat.completions API (not the Responses API) with the Ark "thinking" switch for reasoning mode.
GLM_MODELS = {
    "glm-5-2-260617",
}

# DeepSeek models served through Volces Ark. They use the Responses API and can leverage the
# built-in web_search tool for real-time information.
DEEPSEEK_MODELS = {
    "deepseek-v4-pro-260425",
}

MODEL_CONTEXT_WINDOWS = {
    "doubao-seed-2-1-pro-260628": 262144,
    "doubao-seed-2-1-turbo-260628": 262144,
    "glm-5-2-260617": 1048576,
    "deepseek-v4-pro-260425": 1048576,
}

MODEL_MAX_OUTPUT_TOKENS = {
    "doubao-seed-2-1-pro-260628": DOUBAO_MAX_OUTPUT_TOKENS,
    "doubao-seed-2-1-turbo-260628": DOUBAO_MAX_OUTPUT_TOKENS,
    "glm-5-2-260617": GLM_MAX_OUTPUT_TOKENS,
    "deepseek-v4-pro-260425": DEEPSEEK_MAX_OUTPUT_TOKENS,
}


class LLMStreamIncompleteError(RuntimeError):
    """Retryable provider failure, excluding an ordinary output-token limit."""


class LLMResponseText(str):
    """String-compatible partial answer; never invent missing text or tool arguments."""

    def __new__(cls, content, *, finish_reason):
        result = super().__new__(cls, content or "")
        result.truncated = True
        result.finish_reason = finish_reason
        return result

    def __getnewargs_ex__(self):
        # Context budgeting deep-copies stored conversation messages.
        return (str(self),), {"finish_reason": self.finish_reason}


_OUTPUT_LIMIT_REASONS = frozenset({
    "length", "max_tokens", "max_output_tokens", "max_completion_tokens",
})


def _provider_field(value, name, default=None):
    return value.get(name, default) if isinstance(value, dict) else getattr(value, name, default)


def _output_limit_reason(details):
    reason = details if isinstance(details, str) else _provider_field(details, "reason")
    return reason if isinstance(reason, str) and reason in _OUTPUT_LIMIT_REASONS else None


def _response_text(content, model, reason=None):
    if reason:
        logger.warning(
            "[LLM API][TRUNCATED] model=%s reason=%s; returning %s visible chars "
            "without regeneration or output-budget backoff.",
            model, reason, len(content or ""),
        )
        return LLMResponseText(content, finish_reason=reason)
    return content


class FatalAPIQuotaError(RuntimeError):
    """Raised for permanent quota errors that must stop the entire batch."""


def _is_http_402(error: Exception) -> bool:
    status = getattr(error, "status_code", None)
    if status is None:
        status = getattr(getattr(error, "response", None), "status_code", None)
    return status == 402 or "Error code: 402" in str(error)


def _endpoint_host(opt) -> str:
    return (urlsplit(str(opt.get('base_url') or opt.get('azure_endpoint') or '')).hostname or '').lower()


def _is_ark_endpoint(opt) -> bool:
    host = _endpoint_host(opt)
    return host == "volces.com" or host.endswith(".volces.com")


def default_llm_base_url(config_path) -> str:
    """Read the fallback gateway from configuration rather than source code."""
    override = os.environ.get("OPENAI_BASE_URL") or os.environ.get("AZURE_OPENAI_ENDPOINT")
    if override:
        return override
    path = Path(__file__).resolve().parent / config_path
    config = json.loads(path.read_text(encoding="utf-8"))
    return str(config.get("llm_configuration", {}).get("base_url", ""))


def resolve_llm_provider(opt) -> str:
    """Select the existing Azure or OpenAI-compatible adapter without requiring provider.

    Explicit legacy providers retain their meaning. An api_version alone does
    not imply Azure: OpenRouter and Ark configs also carry that legacy field.
    """
    if _is_ark_endpoint(opt):
        return "aiskt"
    provider = str(opt.get('provider') or '').strip().lower()
    if provider in {"azure", "azure_openai"}:
        return "azure"
    if provider in {"aiskt", "ark", "openai", "openrouter", "openai_compatible"}:
        return "aiskt"
    host = _endpoint_host(opt)
    path = urlsplit(str(opt.get('base_url') or opt.get('azure_endpoint') or '')).path.lower()
    if (opt.get('azure_endpoint')
            or "/openai/deployments/" in path
            or "/modelhub/" in path
            or host.endswith(".openai.azure.com")
            or host.endswith(".cognitiveservices.azure.com")):
        return "azure"
    # Historical adapter labels need not be repeated in source. Their explicit
    # Azure API version still identifies the original transport.
    if provider and opt.get("api_version"):
        return "azure"
    return "aiskt"


def _client_base_url(opt) -> str:
    """Accept API roots or complete endpoint URLs without duplicating their suffix."""
    value = str(opt.get('base_url') or opt.get('azure_endpoint') or '').rstrip('/')
    parts = urlsplit(value)
    path = parts.path.rstrip('/')
    for suffix in ('/chat/completions', '/responses'):
        if path.endswith(suffix):
            path = path[:-len(suffix)]
            break
    return urlunsplit((parts.scheme, parts.netloc, path, parts.query, parts.fragment))


def _resolve_api_key(opt, azure=False):
    if opt.get('api_key'):
        return opt['api_key']
    if opt.get('api_key_env'):
        return os.environ.get(str(opt['api_key_env']))
    host = _endpoint_host(opt)
    if azure:
        return os.environ.get("AZURE_OPENAI_API_KEY") or os.environ.get("OPENAI_API_KEY")
    if _is_ark_endpoint(opt):
        return os.environ.get("ARK_API_KEY")
    if host == "openrouter.ai" or host.endswith(".openrouter.ai"):
        return os.environ.get("OPENROUTER_API_KEY") or os.environ.get("OPENAI_API_KEY")
    return os.environ.get("OPENAI_API_KEY") or os.environ.get("ARK_API_KEY")


def create_llm_client(opt):
    """Shared client factory; callers can keep passing their original option dict."""
    factory = azure_llm_client if resolve_llm_provider(opt) == "azure" else aiskt_llm_client
    return factory(opt)


def azure_llm_client(opt):
    if openai is None:
        raise RuntimeError("The openai package is required to create an LLM API client; install project requirements.")
    client_options = {}
    if 'timeout' in opt:
        client_options['timeout'] = opt['timeout']
    if 'sdk_max_retries' in opt:
        client_options['max_retries'] = opt['sdk_max_retries']
    return openai.AzureOpenAI(
        azure_endpoint=_client_base_url(opt),
        api_version=opt.get('api_version') or os.environ.get("OPENAI_API_VERSION"),
        api_key=_resolve_api_key(opt, azure=True),
        **client_options,
    )


def aiskt_llm_client(opt):
    if openai is None:
        raise RuntimeError("The openai package is required to create an LLM API client; install project requirements.")
    # Bound each provider attempt so a stalled request cannot hold a worker indefinitely.
    # Long-running tasks can override this per configuration when needed.
    # Ark (incl. GLM-5.2) follows the ARK_API_KEY convention: when the caller does not pass an
    # explicit api_key, fall back to the ARK_API_KEY environment variable.
    return openai.OpenAI(
        api_key=_resolve_api_key(opt),
        base_url=_client_base_url(opt) or None,
        timeout=opt.get('timeout', DEFAULT_AISKT_TIMEOUT_SECONDS),
        **({'max_retries': opt['sdk_max_retries']} if 'sdk_max_retries' in opt else {}),
    )








def _get_max_output_tokens(opt: Dict[str, Any], default_value: Optional[int] = None) -> int:
    model = opt['model_name']
    configured = opt.get('max_output_tokens', opt.get('max_tokens'))
    if configured is not None:
        return int(configured)
    if default_value is not None:
        return int(default_value)
    return int(MODEL_MAX_OUTPUT_TOKENS.get(model, RESPONSES_MAX_OUTPUT_TOKENS))


def _backoff_output_budget(opt: Dict[str, Any]) -> int:
    current_budget = _get_max_output_tokens(opt)
    next_budget = max(MIN_RETRY_OUTPUT_TOKENS, current_budget // 2)
    opt['max_output_tokens'] = next_budget
    return next_budget






def _build_responses_input(messages):
    # Responses API: user text uses input_text, assistant text uses output_text.
    input_messages = []
    for i in range(len(messages)):
        role = "user" if i % 2 == 0 else "assistant"
        text_type = "input_text" if role == "user" else "output_text"
        item = {"type": "message", "role": role, "content": [{"type": text_type, "text": messages[i]}]}
        if role == "assistant":
            item["status"] = "completed"
        input_messages.append(item)
    return input_messages


def _build_chat_messages(messages):
    return [{"role": "user" if i % 2 == 0 else "assistant", "content": [{"type": "text", "text": messages[i]}]} for i in range(len(messages))]


def _format_provider_detail(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (str, int, float, bool)):
        return str(value)
    if isinstance(value, dict):
        return ", ".join(f"{key}={_format_provider_detail(val)}" for key, val in value.items())

    details = []
    for attr in ("code", "message", "type", "param", "reason"):
        attr_value = getattr(value, attr, None)
        if attr_value is not None:
            details.append(f"{attr}={attr_value}")
    if details:
        return ", ".join(details)
    return " ".join(str(value).split())


def _raise_if_incomplete_response(response: Any, model: str) -> Optional[str]:
    """Return an explicit output-limit reason; other failures keep normal retries."""
    status = _provider_field(response, "status")
    incomplete_details = _provider_field(response, "incomplete_details")
    error = _provider_field(response, "error")
    if status == "failed" or error:
        raise LLMStreamIncompleteError(
            f"model={model} response failed: {_format_provider_detail(error) or status}"
        )
    if status == "incomplete" or incomplete_details:
        limit_reason = _output_limit_reason(incomplete_details)
        if limit_reason:
            return limit_reason
        reason = _format_provider_detail(incomplete_details) or str(status)
        raise LLMStreamIncompleteError(f"model={model} response incomplete: {reason}")
    return None


def _collect_responses_stream(stream, model: str):
    output_parts = []
    truncation_reason = None
    for event in stream:
        event_type = _provider_field(event, "type", "")
        if event_type == "response.output_text.delta":
            output_parts.append(_provider_field(event, "delta", ""))
        elif event_type == "response.failed":
            response = _provider_field(event, "response")
            raise LLMStreamIncompleteError(
                f"model={model} stream failed: "
                f"{_format_provider_detail(_provider_field(response, 'error') or _provider_field(event, 'error')) or event_type}"
            )
        else:
            response = _provider_field(event, "response")
            if event_type == "response.incomplete" and response is None:
                response = {
                    "status": "incomplete",
                    "incomplete_details": _provider_field(event, "incomplete_details"),
                    "error": _provider_field(event, "error"),
                }
            if response is not None:
                truncation_reason = _raise_if_incomplete_response(response, model) or truncation_reason
    return _response_text("".join(output_parts), model, truncation_reason)


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
    return _response_text("".join(output_parts), model, _output_limit_reason(finish_reason))


def _call_openai_reasoning(client, messages, opt, use_responses_api):
    model = opt['model_name']
    max_output_tokens = _get_max_output_tokens(opt, RESPONSES_MAX_OUTPUT_TOKENS)
    if use_responses_api:
        stream = client.responses.create(
            input=_build_responses_input(messages),
            model=model,
            reasoning={"effort": DEFAULT_REASONING_EFFORT},
            max_output_tokens=max_output_tokens,
            stream=True,
        )
        return _collect_responses_stream(stream, model)
    stream = client.chat.completions.create(
        messages=_build_chat_messages(messages),
        model=model,
        reasoning_effort=DEFAULT_REASONING_EFFORT,
        max_completion_tokens=max_output_tokens,
        stream=True,
    )
    return _collect_chat_stream(stream, model)


def _call_doubao(client, messages, opt, use_responses_api):
    model = opt['model_name']
    thinking = {"thinking": {"type": DOUBAO_THINKING_TYPE}}
    max_output_tokens = _get_max_output_tokens(opt, DOUBAO_MAX_OUTPUT_TOKENS)
    if use_responses_api:
        stream = client.responses.create(
            input=_build_responses_input(messages),
            model=model,
            max_output_tokens=max_output_tokens,
            stream=True,
            extra_body=thinking,
        )
        return _collect_responses_stream(stream, model)
    stream = client.chat.completions.create(
        messages=_build_chat_messages(messages),
        model=model,
        max_tokens=max_output_tokens,
        stream=True,
        extra_body=thinking,
    )
    return _collect_chat_stream(stream, model)


def _call_glm(client, messages, opt, use_responses_api):
    # GLM-5.2 (Volces Ark) follows the official OpenAI-compatible example: always the
    # chat.completions endpoint, never the Responses API (use_responses_api is intentionally
    # ignored here since the Ark base_url would otherwise steer generic calls to /responses).
    # Deep thinking is turned on via the Ark "thinking" switch so reasoning mode is the default;
    # GLM returns its chain-of-thought in a separate reasoning_content field, so collecting the
    # visible delta.content already yields the clean answer.
    model = opt['model_name']
    thinking = {"thinking": {"type": GLM_THINKING_TYPE}}
    max_output_tokens = _get_max_output_tokens(opt, GLM_MAX_OUTPUT_TOKENS)
    stream = opt.get('stream', True)
    if stream:
        response = client.chat.completions.create(
            messages=_build_chat_messages(messages),
            model=model,
            max_tokens=max_output_tokens,
            stream=True,
            extra_body=thinking,
        )
        return _collect_chat_stream(response, model)
    completion = client.chat.completions.create(
        messages=_build_chat_messages(messages),
        model=model,
        max_tokens=max_output_tokens,
        extra_body=thinking,
    )
    finish_reason = getattr(completion.choices[0], "finish_reason", None)
    return _response_text(
        completion.choices[0].message.content, model, _output_limit_reason(finish_reason)
    )


def _deepseek_web_search_tool(opt):
    # Built-in web_search tool (Responses-API only). max_keyword bounds keywords per search
    # round; the model decides whether/when to actually invoke it.
    return [{
        "type": "web_search",
        "max_keyword": opt.get('web_search_max_keyword', DEEPSEEK_WEB_SEARCH_MAX_KEYWORD),
    }]


def _call_deepseek(client, messages, opt, use_responses_api):
    # DeepSeek-V4 (Volces Ark) follows the official example: the Responses API with the built-in
    # web_search tool for real-time info. The Ark base_url already forces use_responses_api True;
    # web_search is only supported on /responses, so this handler always uses responses.create.
    model = opt['model_name']
    tools = _deepseek_web_search_tool(opt)
    max_output_tokens = _get_max_output_tokens(opt, DEEPSEEK_MAX_OUTPUT_TOKENS)
    stream = opt.get('stream', True)
    if stream:
        response = client.responses.create(
            input=_build_responses_input(messages),
            model=model,
            tools=tools,
            max_output_tokens=max_output_tokens,
            stream=True,
        )
        return _collect_responses_stream(response, model)
    response = client.responses.create(
        input=_build_responses_input(messages),
        model=model,
        tools=tools,
        max_output_tokens=max_output_tokens,
    )
    reason = _raise_if_incomplete_response(response, model)
    return _response_text(response.output_text, model, reason)


def _call_generic(client, messages, opt, use_responses_api):
    model = opt['model_name']
    max_output_tokens = _get_max_output_tokens(opt)
    use_stream = opt.get("stream", True)
    if use_responses_api:
        response = client.responses.create(
            input=_build_responses_input(messages),
            model=model,
            max_output_tokens=max_output_tokens,
            stream=use_stream,
        )
        if use_stream:
            return _collect_responses_stream(response, model)
        reason = _raise_if_incomplete_response(response, model)
        return _response_text(response.output_text, model, reason)
    token_parameter = (
        {"max_completion_tokens": max_output_tokens}
        if model in MAX_COMPLETION_TOKENS_MODELS
        else {"max_tokens": max_output_tokens}
    )
    response = client.chat.completions.create(
        messages=_build_chat_messages(messages),
        model=model,
        stream=use_stream,
        **token_parameter,
    )
    if use_stream:
        return _collect_chat_stream(response, model)
    choice = response.choices[0]
    reason = _output_limit_reason(getattr(choice, "finish_reason", None))
    if reason:
        return _response_text(choice.message.content, model, reason)
    content = choice.message.content
    if not content or not content.strip():
        raise LLMStreamIncompleteError(f"model={model} completion has no visible answer.")
    return content


def get_llm_response(messages: List[str], opt):
    base_url = str(opt.get('base_url', ''))
    model_name = opt['model_name']
    is_ark = _is_ark_endpoint(opt)
    use_responses_api = is_ark or urlsplit(base_url).path.rstrip('/').endswith('/responses')
    api_mode = opt.get('api_mode')
    if api_mode is not None:
        if api_mode not in {"responses", "chat_completions"}:
            raise ValueError("api_mode must be responses or chat_completions")
        use_responses_api = api_mode == "responses"
    client = create_llm_client(opt)

    if model_name in DOUBAO_MODELS:
        handler = _call_doubao
    elif model_name in GLM_MODELS:
        handler = _call_glm
    elif model_name in DEEPSEEK_MODELS:
        handler = _call_deepseek
    elif model_name in OPENAI_REASONING_MODELS:
        handler = _call_openai_reasoning
    else:
        handler = _call_generic

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
            request_messages, context_meta = prepare_legacy(
                messages, dict(request_opt, max_output_tokens=_get_max_output_tokens(request_opt)))
            request_opt['max_output_tokens'] = context_meta['output_budget']
            estimated_input_tokens = context_meta['input_tokens']
            start = time.time()
            content = handler(client, request_messages, request_opt, use_responses_api)
            logger.info(
                f"[LLM API Response Time] {time.time() - start} seconds "
                f"(estimated_input_tokens={estimated_input_tokens}, "
                f"max_output_tokens={_get_max_output_tokens(request_opt)})"
            )
            return _strip_reasoning(content)
        except ContextBudgetError:
            raise  # Deterministic local validation must not consume network retries.
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
                    f"OpenRouter HTTP 402 quota error; batch must stop: {e}"
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
