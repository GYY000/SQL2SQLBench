import json
import random
import re
import traceback
from typing import Optional

from func_timeout import func_timeout, FunctionTimedOut
from pyasn1.codec.streaming import peekIntoStream
from sqlglot.expressions import null
from tqdm import tqdm

from sql_gen.generator.element.Point import Point
from sql_gen.generator.generate_pipeline import generate_equivalent_sql_pair, load_no_point_sqls, generate_sql_by_points
from sql_gen.generator.point_parser import parse_point
from sql_gen.generator.rewriter import rewrite_sql
from utils.ExecutionEnv import ExecutionEnv
from utils.db_connector import sql_execute
from utils.tools import get_all_db_name, get_proj_root_path
from verification.verify import tol_order_unaware_compare


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


def transform_placeholders(text):
    pattern = r"\[([^\]]+:[^\]]+|[^\s\]]+)\]"
    return re.sub(pattern, r"<\1>", text)


def verify_points(src_dialect, tgt_dialect, model_name):
    with open(f'{str(get_proj_root_path())}/src/point_gen/{src_dialect}_{tgt_dialect}_{model_name}.json', 'r') as f:
        data = json.load(f)
    res = []
    for p in tqdm(data):
        model_ans = extract_json_from_string(p['model_ans'])
        if model_ans is None:
            continue
        if src_dialect not in model_ans or tgt_dialect not in model_ans:
            continue

        point = {
            "Desc": p['name'],
            "Dialect": {
                "Src": src_dialect,
                "Tgt": tgt_dialect,
            },
            "Type": "EXPRESSION",
            "SrcPattern": transform_placeholders(model_ans[src_dialect]),
            "TgtPattern": transform_placeholders(model_ans[tgt_dialect]),
            "Condition": None,
            "Return": "ANY_VALUE"
        }
        try:
            parsed = parse_point(point)
        except Exception as e:
            print('Parse_error')
            continue
        flag, sql_pair = try_gen_sql_pair(src_dialect, tgt_dialect, point, 3)
        if sql_pair is not None:
            src_sql = sql_pair[src_dialect]
            tgt_sql = sql_pair[tgt_dialect]
            flag1, res1 = sql_execute(src_dialect, get_all_db_name(src_dialect), src_sql)
            flag2, res2 = sql_execute(tgt_dialect, get_all_db_name(tgt_dialect), tgt_sql)
            if flag1 and flag2 and tol_order_unaware_compare(res1, res2):
                res.append({
                    "point": point,
                    "sql_pair": sql_pair,
                })
                with open(f'{str(get_proj_root_path())}/src/point_gen/{src_dialect}_{tgt_dialect}_{model_name}_filter.json',
                          'w') as f:
                    json.dump(res, f, indent=4)


def try_gen_sql_pair(src_dialect: str, tgt_dialect: str, point_def: dict, max_retry_time=5) -> tuple[
    bool, Optional[dict]]:
    i = 0
    while True:
        i += 1
        if i > max_retry_time:
            print(f'No SQL pair can be generated with point requirement {str(point_def)}')
            return False, None
        point_req_list = [{
            "point": parse_point(point_def),
            "num": 1
        }]
        aggressive_flag = False
        db_param = {src_dialect: {}, tgt_dialect: {}}
        try:
            execution_env = ExecutionEnv(src_dialect, get_all_db_name(src_dialect))
            for point_req in point_req_list:
                point = point_req['point']
                assert isinstance(point, Point)
                if point.tag is not None and 'DB PARAMETER' in point.tag:
                    for key, value in point.tag['DB PARAMETER'].items():
                        flag = execution_env.add_param(key, value)
                        db_param.update(key, value)
                        if not flag:
                            raise ValueError('DB Parameter conflict')
            sqls = load_no_point_sqls(src_dialect, tgt_dialect)
            used_sqls = []
            for sql in sqls:
                if sql['db_id'] == 'dw' or sql['db_id'] == 'tpch':
                    continue
                used_sqls.append(sql)

            random.shuffle(used_sqls)
            sql_pair = generate_sql_by_points(point_req_list, aggressive_flag, execution_env, used_sqls, None,
                                              True)
            if sql_pair is None:
                continue
            parsed_points = []
            parsed_point = parse_point(point_def)
            parsed_points.append(parsed_point)
            tgt_sql, all_rewrite_token, rewrite_points = rewrite_sql(src_dialect, tgt_dialect, sql_pair[src_dialect],
                                                                     parsed_points)

            if tgt_sql is None:
                print(f"\033[91m{tgt_sql}\033[0m")
                continue
            sql_pair[tgt_dialect] = tgt_sql
        except Exception as e:
            traceback.print_exc()
            continue
            # raise e

        try:
            flag1, res1 = func_timeout(60, sql_execute, args=(
                src_dialect, get_all_db_name(src_dialect), sql_pair[src_dialect],
                db_param[src_dialect], False, True
            ))
            if not flag1 or len(res1) == 0:
                continue
            flag2, res2 = func_timeout(60, sql_execute, args=(
                tgt_dialect, get_all_db_name(tgt_dialect), sql_pair[tgt_dialect],
                db_param[tgt_dialect], False, True
            ))
            if flag1 and flag2 and tol_order_unaware_compare(res1, res2):
                return True, sql_pair
        except FunctionTimedOut:
            print("SQL execute more than one minutes, skip loop")
            continue
        except Exception as e:
            print(f"execute error: {e}")
            continue
        sql_pair['rewrite_tokens'] = all_rewrite_token
        if flag1 and flag2:
            break
        if sql_pair is None:
            raise ValueError('No SQL pair can be generated')
    return False, sql_pair

# dialects = ['mysql', 'pg', 'oracle']
# for model_names in ['qwen', 'cracksql']:
#     for src_dialect in dialects:
#         for tgt_dialect in dialects:
#             pass
# verify_points('mysql', 'oracle', 'qwen')
