# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: translate_model$
# @Author: 10379
# @Time: 2024/12/13 10:23
import os
import re
import traceback

from model.llm_model import LLMModel
from utils.tools import load_config

config = load_config()
dbg = config['dbg']

deepseek_v3_version = config['deepseek_v3_version']
deepseek_v3_api_base = config['deepseek_v3_api_base']
deepseek_v3_api_key = config['deepseek_v3_api_key']
deepseek_r1_version = config['deepseek_r1_version']
deepseek_r1_api_base = config['deepseek_r1_api_base']
deepseek_r1_api_key = config['deepseek_r1_api_key']
qwen3_coder_api_base = config['qwen3_coder_api_base']
qwen3_coder_id = config['qwen3_coder_id']
qwen3_api_base = config['qwen3_api_base']
qwen3_id = config['qwen3_id']


def init_model(model_id):
    model = None
    #
    # if "gpt-" in model_id:  # gpt-4-turbo, gpt-4o, gpt-4o-mini, gpt-4.1
    #     openai_conf = {"temperature": 0}
    #     api_base = gpt_api_base
    #     api_key = gpt_api_key
    #     model = LLMModel(model_id, openai_conf)
    #     model.load_model(api_base, api_key)

    if model_id == 'deepseek-r1':
        api_base = deepseek_r1_api_base
        api_key = deepseek_r1_api_key
        model = LLMModel(deepseek_r1_version)
        model.load_model(api_base, api_key)

    elif model_id == 'deepseek-v3':
        api_base = deepseek_v3_api_base
        api_key = deepseek_v3_api_key
        model = LLMModel(deepseek_v3_version)
        model.load_model(api_base, api_key)

    elif model_id == 'Qwen3-Coder-30B':
        api_base = qwen3_coder_api_base
        api_key = "EMPTY"
        model = LLMModel(qwen3_coder_id)
        model.load_model(api_base, api_key)

    elif model_id == 'Qwen3-30B':
        api_base = qwen3_api_base
        api_key = "EMPTY"
        model = LLMModel(qwen3_id)
        model.load_model(api_base, api_key)
    else:
        raise ValueError(f"Model {model_id} not supported!")
    return model


def parse_llm_answer(model_id, answer_raw, pattern):
    answer = answer_raw
    try:
        match = re.search(pattern, answer, re.DOTALL)
        if match:
            answer_ori = match.group(1)
            if answer_ori[0] == '"':
                answer_ori = answer_ori[1:]
            if answer_ori[-1] == '"':
                answer_ori = answer_ori[:-1]
            answer_extract = answer_ori.replace('\\\"', '\"').replace('\\n', '\n')
            reasoning = match.group(2).strip('"').replace('\\\"', '\"').replace('\\n', '\n')
            json_content_reflect = {
                "Answer": answer_extract,
                "Reasoning": reasoning
            }
            res = json_content_reflect["Answer"]
        else:
            res = "Answer not returned in the given format!"
        return res
    except Exception as e:
        traceback.print_exc()
        return str(e)
