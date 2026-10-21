import os
import json
import sys
from pathlib import Path
import random
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from collections import Counter
from collections.abc import Mapping, Sequence
from typing import Any, Dict, List, Tuple
from tqdm import tqdm

from llm_api import *

random.seed(42)

DictItem = Dict[str, Any]

openai.api_key = os.environ.get("OPENAI_API_KEY", "")
openai.base_url = os.environ.get("OPENAI_BASE_URL", "https://api.example.com/v1")
model_name = os.environ.get("LLM_MODEL", "your-chat-model")
temperature = 1
retry_times = 10

syn_comp_anomaly_prompt_template = """
# TASK OVERVIEW
You are a professional and highly experienced Database Administrator (DBA) with strong expertise in database internals, workload analysis, performance diagnosis, and root-cause analysis.

Your task is to generate a composite database anomaly definition based on two or three given single-anomaly root cause definitions. A composite anomaly means that the database system is affected by two single anomaly root causes at the same time while running the target workload. You must synthesize these two or three single anomalies into one clear composite anomaly definition.

# INPUT INFORMATION 
You will be given the definitions of two or three single anomalies. Each single anomaly includes its Name, Description, and Type as follows:
{INPUT_INFORMATION}

# OUTPUT FORMAT
You must return exactly one JSON object inside a JSON code block. The JSON object must contain only the following three fields:

1. Name
- The Name must strictly follow this format:
  Name_a + Name_b
  Name_a + Name_b + Name_c (if there are three anomalies)
- Name_a and Name_b must be the exact names of the two input single anomalies.
- Preserve the original spelling, capitalization, and wording of both anomaly names.

2. Description
The Description must include three parts in the following order:
- First, provide a concise definition of the composite anomaly, explaining that it represents the simultaneous presence of the two given root causes.
- Second, describe the combined anomalous phenomena that may appear when the two root causes occur together, especially symptoms that may be amplified, expanded, or become harder to distinguish because of their interaction.
- Third, analyze the potential causal chain behind this composite anomaly, explaining how the two root causes may reinforce, trigger, or aggravate each other and how their interaction can worsen the overall database state.

The Description should be accurate, clear, vendor-neutral, and focused on database root-cause diagnosis. Do not introduce unrelated root causes beyond the two given input anomalies.

3. Type
- The Type value must be exactly one of:
  BOTH, OLTP, OLAP
- Choose the Type according to the workload scenarios in which the composite anomaly can occur.
- If the composite anomaly can occur in both transactional and analytical workloads, use BOTH.
- If it is mainly applicable to transactional workloads, use OLTP.
- If it is mainly applicable to analytical workloads, use OLAP.
- Because a composite anomaly requires both constituent anomalies to occur simultaneously, the composite Type should reflect the common applicable workload scope of the two input anomalies.

Please return only one JSON code block as follows. Do not include explanations, markdown text outside the JSON code block, or additional comments.

```json
{{
    "Name": "Name_a + Name_b",
    "Description": "Name_a : <Concise definition of anomaly a>\nName_b : <Concise definition of anomaly b>\n\n<Description of the combined anomalous phenomena and potential causal chain>",
    "Type": "BOTH"
}}
```
"""

def _freeze_value(value: Any) -> Any:
    if isinstance(value, Mapping):
        return tuple(
            sorted(
                ((key, _freeze_value(sub_value)) for key, sub_value in value.items()),
                key=lambda item: repr(item[0]),
            )
        )

    if isinstance(value, (list, tuple)):
        return tuple(_freeze_value(item) for item in value)

    if isinstance(value, (set, frozenset)):
        return tuple(sorted((_freeze_value(item) for item in value), key=repr))

    try:
        hash(value)
        return value
    except TypeError:
        return repr(value)


def _dict_signature(item: Mapping[str, Any]) -> Tuple[Any, ...]:
    return _freeze_value(item)


def generate_unique_dict_combinations(
    items: Sequence[Mapping[str, Any]],
    max_num: int,
) -> List[List[DictItem]]:
    if items is None:
        return []

    if not isinstance(max_num, int):
        raise TypeError("max_num must be an integer.")

    if max_num < 2 or len(items) < 2:
        return []

    for index, item in enumerate(items):
        if item is None:
            raise ValueError(f"items[{index}] is None; each item must be a dictionary.")
        if not isinstance(item, Mapping):
            raise TypeError(
                f"items[{index}] must be a dictionary-like Mapping, "
                f"got {type(item).__name__}."
            )

    max_len = min(max_num, len(items))

    signature_counts: Counter[Tuple[Any, ...]] = Counter()
    representatives: Dict[Tuple[Any, ...], DictItem] = {}
    ordered_signatures: List[Tuple[Any, ...]] = []

    for item in items:
        signature = _dict_signature(item)

        if signature not in representatives:
            representatives[signature] = dict(item)
            ordered_signatures.append(signature)

        signature_counts[signature] += 1

    results: List[List[DictItem]] = []
    current: List[DictItem] = []

    def backtrack(signature_index: int, remaining: int) -> None:
        if remaining == 0:
            results.append([dict(item) for item in current])
            return

        if signature_index >= len(ordered_signatures):
            return

        available = sum(
            signature_counts[sig]
            for sig in ordered_signatures[signature_index:]
        )
        if available < remaining:
            return

        signature = ordered_signatures[signature_index]
        representative = representatives[signature]
        max_take = min(signature_counts[signature], remaining)

        for take in range(max_take + 1):
            if take > 0:
                current.extend(dict(representative) for _ in range(take))

            backtrack(signature_index + 1, remaining - take)

            if take > 0:
                del current[-take:]

    for length in range(2, max_len + 1):
        backtrack(0, length)

    return results


if __name__ == "__main__":
    single_anomaly_definition_path = "Anomaly_Def/single_anomaly_desp.json"
    with open(single_anomaly_definition_path, "r", encoding="utf-8") as f:
        single_anomaly_definitions = json.load(f)

    comp_anomaly_subsets = generate_unique_dict_combinations(single_anomaly_definitions, max_num=3)
    random.shuffle(comp_anomaly_subsets)

    os.makedirs("Anomaly_Def/3_comp_anomaly_synthesis_messages", exist_ok=True)

    composite_anomaly_definitions = []
    for comp_anomaly_item in tqdm(comp_anomaly_subsets, desc="Synthesizing composite anomaly definitions", total=len(comp_anomaly_subsets)):
        input_information = "\n\n".join(
            f"{{\n\tName: {item['Name']}\n\tDescription: {item['Description']}\n\tType: {item['Type']}\n}}"
            for item in comp_anomaly_item
        )

        prompt = syn_comp_anomaly_prompt_template.format(INPUT_INFORMATION=input_information)

        cnt = 0
        messages = [{"role": "user", "content": prompt}]
        while cnt < retry_times:
            response = get_llm_response([prompt], opt={
                "api_key": openai.api_key,
                "base_url": openai.base_url,
                "model_name": model_name,
            })

            try:
                json_str = response.split("```json")[-1].split("```")[0].strip()
                comp_anomaly_definition = json.loads(json_str)
            except (ValueError, json.JSONDecodeError) as e:
                print(f"[ERROR] Failed to parse JSON from LLM response: {e}. Retrying... (Attempt {cnt + 1}/{retry_times})")
                cnt += 1

                continue

            composite_anomaly_definitions.append(comp_anomaly_definition)
            messages.append({
                "role" : "llm",
                "content" : response
            })

            break

        if cnt >= retry_times:
            print(f"[ERROR] Exceeded maximum retry attempts for response: {response}. Skipping this combination.")
    
        with open("Anomaly_Def/3_comp_anomaly_desp.json", "w", encoding="utf-8") as f:
            json.dump(composite_anomaly_definitions, f, indent=4)
        with open(f"Anomaly_Def/3_comp_anomaly_synthesis_messages/{comp_anomaly_subsets.index(comp_anomaly_item)}.json", "w", encoding="utf-8") as f:
            json.dump(messages, f, indent=4)
        print(f"Synthesized {len(composite_anomaly_definitions)} ({len(comp_anomaly_subsets)}) composite anomaly definitions.")

    gap_num = 20
    id = 1
    output_directory = "Anomaly_Def/anomaly_def_formatted"

    random.shuffle(composite_anomaly_definitions)

    loop_num = len(composite_anomaly_definitions) // gap_num + (1 if len(composite_anomaly_definitions) % gap_num != 0 else 0)
    for i in range(loop_num) :
        start_index = i * gap_num
        end_index = min((i + 1) * gap_num, len(composite_anomaly_definitions))
        subset = composite_anomaly_definitions[start_index:end_index]
        
        print(len(subset))
        with open(f"{output_directory}/{id}.json", 'w') as f :
            json.dump(subset, f, indent=4)
        
        id += 1