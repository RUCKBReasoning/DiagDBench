from multiagents.registry import Registry

llm_registry = Registry(name="LLMRegistry")

from .base import BaseLLM, BaseChatModel, BaseCompletionModel, LLMResult
from .openai import OpenAIChat, OpenAICompletion
from .qwen_vllm import QwenVllmChat

# Local-model adapters require the optional Torch/Transformers dependency set.
# Online OpenAI-compatible diagnosis must remain usable without those packages.
try:
    from .diag_baichuan2_4bit import DiagBaichuan2_4bitChat
    from .diag_baichuan2 import DiagBaichuan2Chat
    from .diag_codellama import DiagCodeLlamaChat
    from .diag_llama2 import DiagLlama2Chat
except ModuleNotFoundError as exc:
    if exc.name not in {"torch", "transformers"}:
        raise

try:
    from .feedback_openai import FeedbackOpenAIChat
    from .feedback_qwen import FeedbackQwenChat
except ModuleNotFoundError:
    # Feedback modes have additional optional SPADE and knowledge-base packages.
    pass
