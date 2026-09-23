"""Semantic document workloads with one frozen token sequence per paired request."""

from __future__ import annotations

import json
from pathlib import Path


RULES = (
    "项目服务手册：收到服务申请后，值班员先核对站点与设备编号，再记录现象、开始时间和业务影响。",
    "备件管理：常用备件按批次存放。领用记录同时保留设备编号、数量和经手人；归还件经过检查后再入库。",
    "现场交接：前一班说明已完成操作、仍需观察的现象和后续联系窗口，后一班核对记录后开始巡检。",
    "资料管理：图纸、操作说明和检修记录以版本号关联。作业前确认批准版本，更新后保留变更原因。",
    "工单安排：紧急故障先恢复关键业务，常规维护按预约顺序执行。跨部门事项由指定联系人协调。",
    "质量复核：维护完成后记录测试条件和结果；复核人检查实际设备状态，确认后将工单转入观察期。",
    "物流协调：到货时间变更时同步现场计划。收货记录包括装箱单、包装状态和实际数量。",
    "周报编制：分别汇总已完成工作、未完成原因和下周安排。延期事项保留原定日期及调整依据。",
    "知识共享：把重复问题整理为简短操作卡，包含适用设备、准备条件、操作步骤和恢复方法。",
    "客户沟通：说明实际进度与下一次更新时间，重要变更进入工单，口头沟通随后补充记录。",
    "能耗管理：每天记录相同时间段的设备运行状态，结合工作量解释变化，异常波动安排复核。",
    "安全交接：检查工作区域、工具清点与设备隔离状态。需要延期的作业重新确认隔离范围。",
)


def document(family_number, phase):
    """A normal service handbook plus dated operational records, not random tokens."""
    branch = f"服务站{family_number:03d}"
    prefix = f"资料包：{branch}年度维护记录。请仅根据资料回答问题。\n"
    if phase == "prefix":
        prefix += f"站点登记：{branch}的固定联系人是林禾，常规巡检日为星期三。\n"
    else:
        prefix += f"新增工单：{branch}本次更换滤芯12件，复核人为周宁，预约时间为下周二。\n"
    paragraphs = [prefix]
    for index in range(180):
        rule = RULES[index % len(RULES)]
        week = index + 1
        completed = 8 + (family_number + index) % 13
        pending = (family_number + index) % 4
        paragraphs.append(
            f"记录{week:03d}，{branch}运行观察：{rule}"
            f"该观察期完成{completed}项常规工作，另有{pending}项等待备件或预约。"
            "值班员核对现场记录并更新交接表，下一观察期按工单优先级继续处理。\n"
        )
    return "".join(paragraphs)


def encode(tokenizer, text):
    return list(tokenizer.encode(text, add_special_tokens=False))


def chat_frame(tokenizer):
    """Use the model's local conversation format while freezing final token IDs."""
    if not getattr(tokenizer, "chat_template", None):
        raise ValueError("Select an instruction model with an existing local chat template")
    marker = "E004_DOCUMENT_CONTENT"
    rendered = tokenizer.apply_chat_template([{"role": "user", "content": marker}],
                                             tokenize=False, add_generation_prompt=True)
    if rendered.count(marker) != 1:
        raise ValueError("The chat template must retain the document as one user message")
    before, after = rendered.split(marker)
    return encode(tokenizer, before), encode(tokenizer, after)


def framed_prompt(tokenizer, frame, prefix_text, suffix_text, question, prefix_length):
    before, after = frame
    question_tokens = encode(tokenizer, question)
    prefix = before + encode(tokenizer, prefix_text)[:prefix_length - len(before)]
    suffix_room = 8192 - prefix_length - len(question_tokens) - len(after)
    suffix = encode(tokenizer, suffix_text)[:suffix_room] + question_tokens + after
    if len(before) >= prefix_length or suffix_room <= 0 or len(prefix) != prefix_length or len(prefix + suffix) != 8192:
        raise ValueError("Fixture text or chat template does not fit the fixed token budget")
    return prefix + suffix


def build_cases(tokenizer):
    cases = []
    frame = chat_frame(tokenizer)
    question = "\n请结合已有手册和新增工单，写一份维护交接摘要，说明联系窗口、巡检安排、工单执行、资料复核和下一步工作。"
    for family, prefix_length in (("long_prefix", 6144), ("long_suffix", 2048)):
        for index in range(20):
            number = index + (0 if family == "long_prefix" else 100)
            tokens = framed_prompt(tokenizer, frame, document(number, "prefix"),
                                   document(number, "suffix"), question, prefix_length)
            cases.append({"case_id": f"{family}-{index:02d}", "family": family,
                          "prefix_tokens": prefix_length, "prompt": tokens,
                          "output_tokens": 128})
    # Interleave distinct prefix lengths; all modes reuse these exact token IDs.
    return [cases[offset + i] for i in range(20) for offset in (0, 20)]


def correctness_cases(tokenizer, block_size):
    cases = []
    frame = chat_frame(tokenizer)
    facts = (("林禾", "12", "周宁"), ("陈松", "18", "王岚"),
             ("李楠", "24", "赵敏"), ("宋岩", "16", "许晨"))
    for index in range(4):
        contact, quantity, reviewer = facts[index]
        prefix_length = 6144 if index % 2 == 0 else 2048
        prefix_length = (prefix_length // block_size) * block_size
        prefix_text = document(800 + index, "prefix").replace("林禾", contact)
        suffix_text = document(800 + index, "suffix").replace("12件", quantity + "件").replace("周宁", reviewer)
        question = "\n请回答本站固定联系人、本次新增工单的滤芯更换数量和复核人，仅输出“联系人，数量，复核人”。"
        tokens = framed_prompt(tokenizer, frame, prefix_text, suffix_text, question, prefix_length)
        cases.append({"case_id": f"qa-{index}", "family": "long_prefix" if index % 2 == 0 else "long_suffix",
                      "prefix_tokens": prefix_length, "prompt": tokens,
                      "output_tokens": 128, "expected": [contact, quantity, reviewer]})
    return cases


def answer_matches(text, expected):
    return isinstance(text, str) and all(part in text for part in expected)


def model_layout(model_path, block_size):
    config = json.loads((Path(model_path) / "config.json").read_text(encoding="utf-8"))
    required = ("num_hidden_layers", "num_key_value_heads", "num_attention_heads", "hidden_size")
    if any(not isinstance(config.get(name), int) or config[name] <= 0 for name in required):
        raise ValueError("The fixed E004 workload requires a declared dense GQA model layout")
    if config["num_key_value_heads"] % 2:
        raise ValueError("The selected KV head count must divide across TP=2")
    head_dim = config.get("head_dim", config["hidden_size"] // config["num_attention_heads"])
    per_token = 2 * config["num_hidden_layers"] * config["num_key_value_heads"] * head_dim * 2
    return {"layers": config["num_hidden_layers"], "kv_heads": config["num_key_value_heads"],
            "head_dim": head_dim, "dtype_bytes": 2, "tp": 2,
            "kv_bytes_per_token": per_token, "block_size": block_size,
            "single_layer_rank_block_bytes": 2 * block_size * (config["num_key_value_heads"] // 2) * head_dim * 2}


def required_storage_bytes(layout):
    # No deletion API is assumed. Reserve all 320 formal requests plus every
    # calibration/QA namespace and 25% layout/metadata headroom. Prefix preparation
    # reuses the same keys, so it is already included in the complete prompt size.
    return int((320 + 20 + 16 + 4) * 8192 * layout["kv_bytes_per_token"] * 1.25)


def make_manifest(model_path, block_size):
    from transformers import AutoTokenizer
    tokenizer = AutoTokenizer.from_pretrained(model_path, local_files_only=True,
                                              trust_remote_code=False)
    layout = model_layout(model_path, block_size)
    if layout["single_layer_rank_block_bytes"] > 8 * 1024**2:
        raise ValueError("One legal KV block exceeds the approved 8 MiB task limit")
    return {"layout": layout, "prompt_format": "local_tokenizer_chat_template_user_message",
            "cases": build_cases(tokenizer),
            "correctness": correctness_cases(tokenizer, block_size)}
