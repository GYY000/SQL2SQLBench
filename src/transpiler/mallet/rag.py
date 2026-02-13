# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: gen_func_point.py$
# @Author: 10379
# @Time: 2024/12/29 11:41
import json
import os.path
import random
import re

from litellm.proxy.proxy_server import file_path
from tqdm import tqdm

from exp_script.param_script import fetch_db_param
from model.model_init import init_model
from point_gen.prompt import sys_prompt_bat, user_prompt_bat
import chromadb
from chromadb.utils import embedding_functions

from sql_gen.generator.element.Point import Point
from sql_gen.generator.point_parser import parse_point
from sql_gen.generator.rewriter import rewrite_sql
from transpiler.mallet.prompt import sys_prompt, user_prompt
from utils.db_connector import sql_execute
from utils.tools import get_all_db_name, get_proj_root_path
from verification.verify import tol_order_unaware_compare

documents = {}


def load_function_name_desc_pair_list(dialect: str):
    if dialect == 'mysql':
        file_path = os.path.join(get_proj_root_path(), 'src', 'transpiler', 'mallet', 'mysql_8_kb.json')
    elif dialect == 'oracle':
        file_path = os.path.join(get_proj_root_path(), 'src', 'transpiler', 'mallet', 'oracle_11_kb.json')
    elif dialect == 'pg':
        file_path = os.path.join(get_proj_root_path(), 'src', 'transpiler', 'mallet', 'pg_14_kb.json')
    else:
        assert False
    with open(file_path, 'r') as json_file:
        data = json.load(json_file)
    if dialect in documents:
        return documents[dialect]
    else:
        res = []
        for item in data:
            if item['type'] == 'function':
                res.append({
                    "name": item['keyword'],
                    "desc": item['description']
                })
        documents[dialect] = res
        return res


def get_model_id(model_name: str):
    if 'qwen' in model_name:
        return 'qwen'
    else:
        return 'cracksql'


def keyword_search(src_name, src_desc, tgt_dialect, top_k=5):
    tgt_functions = load_function_name_desc_pair_list(tgt_dialect)
    clean_name = before_first_paren(src_name).lower()

    scored_results = []

    for item in tgt_functions:
        tgt_name = item['name'].lower()
        tgt_desc = item['desc'].lower()
        score = 0
        if clean_name == before_first_paren(tgt_name):
            score += 100
        elif clean_name in tgt_name or tgt_name in clean_name:
            score += 50
        if clean_name in tgt_desc:
            score += 20
        if score > 0:
            scored_results.append({
                "name": item['name'],
                "desc": item['desc'],
                "score": score
            })
    scored_results.sort(key=lambda x: x['score'], reverse=True)
    if len(scored_results) >= top_k:
        return scored_results[:top_k]
    else:
        return scored_results


def fetch_similar_tgt_function(src_dialect, src_sql: str,
                               tgt_dialect: str, embedding_model: str, top_k: int = 5):
    chroma_db_path = f'/home/gyy/SQL2SQL_Bench/src/transpiler/mallet/chroma_store'
    collection_name = f"target_functions_{tgt_dialect}_{get_model_id(embedding_model)}"
    client = chromadb.PersistentClient(path=chroma_db_path)
    ef = embedding_functions.SentenceTransformerEmbeddingFunction(
        model_name=embedding_model
    )
    collection = client.get_or_create_collection(
        name=collection_name,
        embedding_function=ef,
        metadata={"hnsw:space": "cosine"}
    )
    search_query = f"In {src_dialect}, the SQL code or function: {src_sql}. What is the equivalent function in {tgt_dialect}?"
    results = collection.query(
        query_texts=[search_query],
        n_results=top_k,
        include=["metadatas", "documents", "distances"]
    )
    kw_hits = []
    if results["metadatas"]:
        for meta, dist, doc in zip(results["metadatas"][0],
                                   results["distances"][0],
                                   results["documents"][0]):
            kw_hits.append({
                "name": meta.get("name"),
                "desc": meta.get("desc"),
                "distance": dist,
                "doc": doc,
            })
    return kw_hits


def before_first_paren(s: str) -> str:
    return s.split("(", 1)[0]


def load_rules(src_dialect, tgt_dialect):
    with open(f"/home/gyy/SQL2SQL_Bench/src/transpiler/mallet/"
              f"generated_rules/{src_dialect}_{tgt_dialect}_point.json") as f:
        data = json.load(f)
    points = []
    for p in data:
        try:
            point = parse_point(p)
            points.append(point)
        except Exception as e:
            print(e)
    return points


def extract_json_from_string(text):
    pattern = r"```json\s*(.*?)\s*```"
    match = re.search(pattern, text, re.DOTALL)

    if match:
        json_str = match.group(1)
        try:
            # 将提取到的字符串解析为 Python 字典
            data = json.loads(json_str)
            return data
        except json.JSONDecodeError as e:
            return None
    else:
        return None


def try_gen_rules(src_dialect, tgt_dialect, db_param, sql, error_info):
    embedding_path = os.path.join(get_proj_root_path(), 'src', 'transpiler', 'cracksql_driver', 'all-MiniLM-L6-v2')
    similar_info = fetch_similar_tgt_function(src_dialect, sql, tgt_dialect, embedding_path)
    ref_func_descriptions = ''
    for item in similar_info:
        ref_func_descriptions += f"  - {item['name']}: `{item['desc']}`\n"
    formatted_sys_prompt = sys_prompt.format(src_dialect=src_dialect, tgt_dialect=tgt_dialect)
    formatted_user_prompt = user_prompt.format(src_dialect=src_dialect, tgt_dialect=tgt_dialect, error_info=error_info,
                                               db_param=json.dumps(db_param, indent=2), sql=sql,
                                               retrieved_info=similar_info)
    model = init_model('DeepSeek-V3.2')
    ans = model.trans_func([], formatted_sys_prompt, formatted_user_prompt)
    point_defs = extract_json_from_string(ans)

    """
    "Tag": {
        "DB PARAMETER": {
          "{src_dialect}": {
            "param_name": "param_value",
            ...
          },
          "{tgt_dialect}": {
            "param_name": "param_value",
            ...
          }
        }
      }
    """
    if point_defs is not None:
        for p in point_defs:
            final_db_param = {src_dialect: {}, tgt_dialect: {}}
            if 'Tag' in p and 'DB PARAMETER' in p['Tag']:
                for dialect, value in p['Tag']['DB PARAMETER'].items():
                    for param_name, param_value in value.items():
                        if param_name in db_param[dialect] and param_value == db_param[dialect][param_name]:
                            final_db_param[dialect][param_name] = param_value
            p["Type"] = "EXPRESSION"
            p['Dialect'] = {'Src': src_dialect, 'Tgt': tgt_dialect}
            p["SrcPattern"] = p[src_dialect]
            p["TgtPattern"] = p[tgt_dialect]
            p["Condition"] = None
            p["Return"] = 'ANY_VALUE'
            p['Tag'] = {'DB PARAMETER': final_db_param}
            p.pop(src_dialect)
            p.pop(tgt_dialect)
    return point_defs


def upload_new_rules(rules: list, src_dialect: str, tgt_dialect: str):
    rules_path = os.path.join(get_proj_root_path(), f'src/transpiler/mallet/generated_rules/{src_dialect}_{tgt_dialect}_point.json')
    with open(rules_path, 'r') as f:
        data = json.load(f)
    data = data + rules
    with open(rules_path, 'w') as f:
        json.dump(data, f, indent=2)


def transpile_mallet(sql_dict: dict, frozen: bool, retry_time=3):
    src_dialect = None
    tgt_dialect = None
    if 'Dialect' in sql_dict:
        src_dialect = sql_dict['Dialect']['Src']
        tgt_dialect = sql_dict['Dialect']['Tgt']
        src_sql = sql_dict['SQL'][src_dialect]
    else:
        for key, value in sql_dict.items():
            if src_dialect is None:
                src_dialect = key
            elif tgt_dialect is None:
                tgt_dialect = key
        src_sql = sql_dict[src_dialect]

    if 'SQL' in sql_dict:
        db_points = sql_dict['SQL']['points']
    else:
        db_points = sql_dict['points']
    db_param = {
        src_dialect: {},
        tgt_dialect: {}
    }

    for point1 in db_points:
        if isinstance(point1, dict):
            point_db_param = fetch_db_param(point1['point'], src_dialect, tgt_dialect)
        else:
            point_db_param = fetch_db_param(point1, src_dialect, tgt_dialect)
        for key, value1 in point_db_param.items():
            db_param[key].update(value1)

    points = load_rules(src_dialect, tgt_dialect)
    to_pop_points = []
    for rule in points:
        assert isinstance(rule, Point)
        if rule.tag is not None:
            if 'DB PARAMETER' in rule.tag:
                flag = True
                for dialect, params in rule.tag['DB PARAMETER'].items():
                    for param_name, param_value in params.items():
                        if param_name in db_param[dialect]:
                            if db_param[dialect][param_name] != param_value:
                                flag = False
                if not flag:
                    to_pop_points.append(rule)
    for p in to_pop_points:
        points.remove(p)
    rewritten_sql, all_rewrite_token, all_rewrite_points = rewrite_sql(src_dialect, tgt_dialect, src_sql, points)
    if frozen:
        return rewritten_sql
    flag, res = sql_execute(src_dialect, get_all_db_name(src_dialect), src_sql, db_param[src_dialect])
    if not flag:
        print('Error SQL')
        return None
    i = 0
    cur_sql = rewritten_sql
    not_retry = True
    new_rules = []
    while i < retry_time:
        i += 1
        flag1, res1 = sql_execute(tgt_dialect, get_all_db_name(tgt_dialect), cur_sql, db_param[tgt_dialect])
        if not flag1:
            error_info = f'Syntax Error: {res1}'
        else:
            if tol_order_unaware_compare(res, res1):
                upload_new_rules(new_rules, src_dialect, tgt_dialect)
                return cur_sql
            else:
                error_info = f'Inconsistent Results'
        if i >= retry_time * 0.8 and not_retry:
            cur_sql = src_sql
            not_retry = False
        new_rules = try_gen_rules(src_dialect, tgt_dialect, db_param, cur_sql, error_info)
        new_usable_rules = []
        for p in new_rules:
            try:
                point = parse_point(p)
                new_usable_rules.append(point)
            except Exception as e:
                print(e)
        to_pop_points = []
        for rule in new_usable_rules:
            assert isinstance(rule, Point)
            if rule.tag is not None:
                if 'DB PARAMETER' in rule.tag:
                    flag = True
                    for dialect, params in rule.tag['DB PARAMETER'].items():
                        for param_name, param_value in params.items():
                            if param_name in db_param[dialect]:
                                if db_param[dialect][param_name] != param_value:
                                    flag = False
                    if not flag:
                        to_pop_points.append(rule)
        for p in to_pop_points:
            new_usable_rules.remove(p)
        for p in new_usable_rules:
            assert isinstance(p, Point)
        try:
            cur_sql, all_rewrite_token, all_rewrite_points = rewrite_sql(src_dialect, tgt_dialect, src_sql,
                                                                         points + new_usable_rules)
        except Exception as e:
            cur_sql = src_sql
    return cur_sql


def pre_rule_generation(rules_gen_path):
    dialects = ['mysql', 'pg', 'oracle']
    for src_dialect in dialects:
        for tgt_dialect in dialects:
            if src_dialect == tgt_dialect:
                continue
            if src_dialect == 'mysql':
                continue
            if not (src_dialect == 'pg' or tgt_dialect == 'pg'):
                continue
            with open(rules_gen_path, 'r') as f:
                sqls = json.load(f)
            for sql in tqdm(sqls):
                try:
                    transpile_mallet(sql, False)
                except Exception as e:
                    print(e)
