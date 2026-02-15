# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: tran_dataset$
# @Author: 10379
# @Time: 2025/8/17 8:13
import json
import os
import math
from concurrent.futures import ThreadPoolExecutor

from accelerate.test_utils.scripts.test_script import print_on
from tqdm import tqdm

from exp_script.param_script import fetch_db_param
from transpiler.transpile import model_translate, cracksql_translate, transfer_sql_sqline, translate_sqlglot, \
    ora2pg_tran, feed_back_model, translate_dspy
from utils.db_connector import sql_dependent_execute, get_tran_db_name
from utils.tools import get_all_db_name
from verification.verify import post_process_for_reserved_keyword

# The scripts to translate a dataset
def tran_cracksql(point, num, multi_mode=False):
    if multi_mode:
        src_dialect = None
        tgt_dialect = None
        test_sql = None
        ans_sql = None
        for key, value in point.items():
            if src_dialect is None:
                src_dialect = key
                test_sql = value
            elif tgt_dialect is None:
                tgt_dialect = key
                ans_sql = value
    else:
        src_dialect = point['Dialect']['Src']
        tgt_dialect = point['Dialect']['Tgt']
        test_sql = point['SQL'][src_dialect]
        ans_sql = point['SQL'][tgt_dialect]
    out_dir = f'/home/gyy/SQL2SQL_Bench/dataset/crackout_dir/dir{num}'
    if not os.path.exists(out_dir):
        os.mkdir(out_dir)
    flag, res_cracksql, lift_history, used_pieces = cracksql_translate(test_sql, src_dialect, tgt_dialect,
                                                                       get_all_db_name(tgt_dialect),
                                                                       'deepseek-r1-250528', out_dir)
    if flag:
        res_cracksql = post_process_for_reserved_keyword(res_cracksql, src_dialect, tgt_dialect)
        return {
            "tran_res": res_cracksql,
            "used_pieces": used_pieces,
            "lift_history": lift_history
        }
    else:
        return {
            "tran_res": res_cracksql,
            "verify_res": {
                "execution": False,
                "error": 'Error Occurred',
                "formal": None,
                "manual": False
            }
        }


def tran_dataset_cracksql(dataset_path, multi_mode=False):
    if dataset_path.endswith('exp_res.json') or dataset_path.endswith('exp_res_flt.json') or dataset_path.endswith(
            'dedup.json'):
        res_path = dataset_path
    else:
        res_path = f'{dataset_path.removesuffix(".json")}_exp_res.json'
    if os.path.exists(res_path):
        with open(res_path, 'r') as file:
            queries = json.load(file)
    else:
        with open(dataset_path, 'r') as file:
            queries = json.load(file)
    to_tran_queries = []
    for query in queries:
        if 'exp_res' not in query or 'cracksql' not in query['exp_res']:
            to_tran_queries.append(query)
    divide = 4
    slice_len = math.ceil(len(to_tran_queries) / divide)
    all_queries = []
    for i in range(divide):
        if i == 0:
            all_queries.append(to_tran_queries[:slice_len])
        elif i == divide - 1:
            all_queries.append(to_tran_queries[i * slice_len:])
        else:
            all_queries.append(to_tran_queries[i * slice_len: (i + 1) * slice_len])
    all_res = queries
    for i in tqdm(range(len(all_queries[0]))):
        future = {}
        tran_flag = {}
        with ThreadPoolExecutor(max_workers=divide) as executor:
            for j in range(divide):
                if multi_mode:
                    tran_flag[j] = i < len(all_queries[j]) and (
                            'exp_res' not in all_queries[j][i] or
                            'cracksql' not in all_queries[j][i]['exp_res'])
                else:
                    tran_flag[j] = i < len(all_queries[j]) and all_queries[j][i]['SQL'] is not None and (
                            'exp_res' not in all_queries[j][i] or 'cracksql' not in all_queries[j][i]['exp_res'])
                if tran_flag[j]:
                    future[j] = executor.submit(tran_cracksql, all_queries[j][i], j, multi_mode)
            for j in range(divide):
                if tran_flag[j]:
                    if 'exp_res' not in all_queries[j][i]:
                        all_queries[j][i]['exp_res'] = {}
                    all_queries[j][i]['exp_res']['cracksql'] = future[j].result()
                with open(res_path, 'w') as f:
                    json.dump(all_res, f, indent=4)


def tran_query(point, number, cracksql_flag):
    src_dialect = point['Dialect']['Src']
    tgt_dialect = point['Dialect']['Tgt']
    test_sql = point['SQL'][src_dialect]
    ans_sql = point['SQL'][tgt_dialect]
    db_param = {
        src_dialect: {},
        tgt_dialect: {}
    }
    assert isinstance(point, dict)
    exp_res = point.get('exp_res', {})
    for point1 in point['SQL']['points']:
        if isinstance(point1, dict):
            point_name = point1['point']
        else:
            point_name = point1
        point_db_param = fetch_db_param(point_name, src_dialect, tgt_dialect)
        for key, value in point_db_param.items():
            db_param[key].update(value)
    flag_res_sql, res_res_sql = sql_dependent_execute(tgt_dialect,
                                                      get_all_db_name(tgt_dialect),
                                                      ans_sql,
                                                      db_param[tgt_dialect])
    if not flag_res_sql:
        return None
    if not cracksql_flag:
        max_workers = 4
    else:
        max_workers = 5
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        if 'DS' not in exp_res:
            future_translate = executor.submit(model_translate, point['SQL'], src_dialect, tgt_dialect,
                                               'deepseek-r1',
                                               db_param, number)
        if 'Deepseek-v3' not in exp_res:
            future_ds_v3 = executor.submit(model_translate, point['SQL'], src_dialect, tgt_dialect, 'deepseek-v3',
                                           db_param,
                                           number)

        if 'Qwen3-Coder-30B' not in exp_res:
            future_qwen_coder = executor.submit(model_translate, point['SQL'], src_dialect, tgt_dialect,
                                                'Qwen3-Coder-30B',
                                                db_param,
                                                number)

        if 'Qwen3-30B' not in exp_res:
            future_qwen = executor.submit(model_translate, point['SQL'], src_dialect, tgt_dialect,
                                                'Qwen3-30B',
                                                db_param,
                                                number)

        if cracksql_flag and 'cracksql' not in exp_res:
            future_cracksql = executor.submit(cracksql_translate, test_sql, src_dialect, tgt_dialect,
                                              get_all_db_name(tgt_dialect), 'deepseek-r1-250528', number)

        if 'sqlines' not in exp_res:
            flag, res = transfer_sql_sqline(test_sql, src_dialect, tgt_dialect, number)
            res = post_process_for_reserved_keyword(res, src_dialect, tgt_dialect)
            if flag:
                exp_res['sqlines'] = {
                    "tran_res": res,
                }
        if 'sqlglot' not in exp_res:
            flag, res = translate_sqlglot(test_sql, src_dialect, tgt_dialect)
            res = post_process_for_reserved_keyword(res, src_dialect, tgt_dialect)
            if flag:
                exp_res['sqlglot'] = {
                    "tran_res": res
                }
            else:
                exp_res['sqlglot'] = {
                    "tran_res": res,
                    "verify_res": {
                        "execution": False,
                        "error": res,
                        "formal": None,
                        "manual": False
                    }
                }
        if tgt_dialect == 'pg' and 'ora2pg' not in exp_res:
            res = ora2pg_tran(test_sql, src_dialect, tgt_dialect, get_all_db_name(tgt_dialect), number)
            res = post_process_for_reserved_keyword(res, src_dialect, tgt_dialect)
            exp_res['ora2pg'] = {
                "tran_res": res,
            }

        if 'Deepseek-v3' not in exp_res:
            res_v3 = future_ds_v3.result()
            res_v3 = post_process_for_reserved_keyword(res_v3, src_dialect, tgt_dialect)
            exp_res['Deepseek-v3'] = {
                "tran_res": res_v3
            }
        if 'DS' not in exp_res:
            res_ds = future_translate.result()
            res_ds_processed = post_process_for_reserved_keyword(res_ds, src_dialect, tgt_dialect)
            exp_res['DS'] = {
                "tran_res": res_ds_processed,
            }
        if 'Qwen3-30B' not in exp_res:
            res_qwen_coder = future_qwen.result()
            res_qwen_coder_processed = post_process_for_reserved_keyword(res_qwen_coder, src_dialect, tgt_dialect)
            exp_res['Qwen3-30B'] = {
                "tran_res": res_qwen_coder_processed,
            }
        if 'Qwen3-Coder-30B' not in exp_res:
            res_qwen_coder = future_qwen_coder.result()
            res_qwen_coder_processed = post_process_for_reserved_keyword(res_qwen_coder, src_dialect, tgt_dialect)
            exp_res['Qwen3-Coder-30B'] = {
                "tran_res": res_qwen_coder_processed,
            }
        if cracksql_flag and 'cracksql' not in exp_res:
            flag, res_cracksql, lift_history, used_pieces = future_cracksql.result()
            if flag:
                res_cracksql = post_process_for_reserved_keyword(res_cracksql, src_dialect, tgt_dialect)
                exp_res['cracksql'] = {
                    "tran_res": res_cracksql,
                    "used_pieces": used_pieces,
                    "lift_history": lift_history
                }
            else:
                exp_res['cracksql'] = {
                    "tran_res": res_cracksql,
                    "verify_res": {
                        "execution": False,
                        "error": 'Error Occurred',
                        "formal": None,
                        "manual": False
                    }
                }
    return exp_res


def leave_out_exp_res(exp_res, src_dialect, tgt_dialect):
    if tgt_dialect not in ['sqlserver', 'snowflake'] and 'cracksql' not in exp_res:
        return True
    elif tgt_dialect == 'pg' and 'ora2pg' not in exp_res:
        return True
    elif tgt_dialect == 'snowflake' and 'sqlines' not in exp_res:
        return True
    elif 'Deepseek-v3' not in exp_res or 'DS' not in exp_res or 'Qwen3-30B' not in exp_res or 'Qwen3-Coder-30B' not in exp_res:
        return True
    elif 'Qwen3-30B' not in exp_res or 'Qwen3-Coder-30B' not in exp_res:
        return True
    for key in ['sqlines', 'sqlglot', 'Deepseek-v3', 'DS', 'cracksql']:
        if key not in exp_res:
            return True
    return False


def tran_dataset(dataset_path: str, name: str, add_cracksql_flag):
    if dataset_path.endswith('exp_res.json') or dataset_path.endswith('exp_res_flt.json'):
        res_path = dataset_path
    else:
        res_path = f'{dataset_path.removesuffix(".json")}_exp_res.json'
    if os.path.exists(res_path):
        with open(res_path, 'r') as file:
            queries = json.load(file)
    else:
        with open(dataset_path, 'r') as file:
            queries = json.load(file)
    to_tran_queries = []
    for i in range(len(queries)):
        if 'exp_res' not in queries[i] or leave_out_exp_res(queries[i]['exp_res']):
            to_tran_queries.append(queries[i])
    divide = 4
    slice_len = math.ceil(len(to_tran_queries) / divide)
    queries1 = to_tran_queries[:slice_len]
    queries2 = to_tran_queries[slice_len:2 * slice_len]
    queries3 = to_tran_queries[2 * slice_len:3 * slice_len]
    queries4 = to_tran_queries[3 * slice_len:]
    for i in tqdm(range(len(queries1))):
        with ThreadPoolExecutor(max_workers=divide) as executor:
            tran_flag1 = i < len(queries1) and (
                    'exp_res' not in queries1[i] or leave_out_exp_res(queries1[i]['exp_res'])) and 'SQL' in \
                         queries1[i] and queries1[i][
                             'SQL'] is not None
            tran_flag2 = i < len(queries2) and (
                    'exp_res' not in queries2[i] or leave_out_exp_res(queries2[i]['exp_res'])) and 'SQL' in \
                         queries2[i] and queries2[i][
                             'SQL'] is not None
            tran_flag3 = i < len(queries3) and (
                    'exp_res' not in queries3[i] or leave_out_exp_res(queries3[i]['exp_res'])) and 'SQL' in queries3[
                             i] and queries3[i][
                             'SQL'] is not None
            tran_flag4 = len(queries4) > i and (
                    'exp_res' not in queries4[i] or leave_out_exp_res(queries4[i]['exp_res'])) and 'SQL' in queries4[
                             i] and queries4[i][
                             'SQL'] is not None
            if tran_flag1:
                future1 = executor.submit(tran_query, queries1[i], f'{name}1', add_cracksql_flag)
            if tran_flag2:
                future2 = executor.submit(tran_query, queries2[i], f'{name}2', add_cracksql_flag)
            if tran_flag3:
                future3 = executor.submit(tran_query, queries3[i], f'{name}3', add_cracksql_flag)
            if tran_flag4:
                future4 = executor.submit(tran_query, queries4[i], f'{name}4', add_cracksql_flag)
            if tran_flag1:
                queries1[i]['exp_res'] = future1.result()
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)
            if tran_flag2:
                queries2[i]['exp_res'] = future2.result()
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)
            if tran_flag3:
                queries3[i]['exp_res'] = future3.result()
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)
            if tran_flag4:
                queries4[i]['exp_res'] = future4.result()
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)
            # with open(f'{dataset_path}_exp_res.json', 'w') as f:
            #     json.dump(all_res, f, indent=4)


def tran_multi_query(sql_dict, number, cracksql_flag, tran_flag=False):
    src_dialect = None
    tgt_dialect = None
    test_sql = None
    ans_sql = None
    for key, value in sql_dict.items():
        if src_dialect is None:
            src_dialect = key
            test_sql = value
        elif tgt_dialect is None:
            tgt_dialect = key
            ans_sql = value
    db_param = {
        src_dialect: {},
        tgt_dialect: {}
    }
    assert isinstance(sql_dict, dict)
    exp_res = sql_dict.get('exp_res', {})
    for point1 in sql_dict['points']:
        point_db_param = fetch_db_param(point1['point'], src_dialect, tgt_dialect)
        for key, value in point_db_param.items():
            db_param[key].update(value)
    if tran_flag:
        db_name = get_tran_db_name(get_all_db_name(src_dialect))
    else:
        db_name = get_all_db_name(src_dialect)
    flag_res_sql, res_res_sql = sql_dependent_execute(tgt_dialect,
                                                      db_name,
                                                      ans_sql,
                                                      db_param[tgt_dialect])
    if not flag_res_sql:
        return None
    if not cracksql_flag:
        max_workers = 4
    else:
        max_workers = 5
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_translate = executor.submit(model_translate, sql_dict, src_dialect, tgt_dialect,
                                           'deepseek-r1',
                                           db_param, number)
        # future_feedback = executor.submit(feed_back_model, sql_dict, src_dialect, tgt_dialect, db_param, 3,
        #                                   'deepseek-r1', number)
        # future_ds_llama_translate = executor.submit(model_translate, sql_dict, src_dialect, tgt_dialect,
        #                                             'deepseek-r1-distill-llama-70b',
        #                                             db_param, number)
        future_ds_v3 = executor.submit(model_translate, sql_dict, src_dialect, tgt_dialect, 'deepseek-v3', db_param,
                                       number)
        # future_qwen3 = executor.submit(model_translate, sql_dict, src_dialect, tgt_dialect,'Qwen3-30B', db_param, number)

        if cracksql_flag:
            future_cracksql = executor.submit(cracksql_translate, test_sql, src_dialect, tgt_dialect,
                                              get_all_db_name(tgt_dialect), 'deepseek-r1-250528', number)

        if 'snowflake' not in [src_dialect, tgt_dialect]:
            if 'sqlines' not in exp_res:
                flag, res = transfer_sql_sqline(test_sql, src_dialect, tgt_dialect, number)
                res = post_process_for_reserved_keyword(res, src_dialect, tgt_dialect)
                if flag:
                    exp_res['sqlines'] = {
                        "tran_res": res,
                    }
        if 'sqlglot' not in exp_res:
            flag, res = translate_sqlglot(test_sql, src_dialect, tgt_dialect)
            res = post_process_for_reserved_keyword(res, src_dialect, tgt_dialect)
            if flag:
                exp_res['sqlglot'] = {
                    "tran_res": res
                }
            else:
                exp_res['sqlglot'] = {
                    "tran_res": res,
                    "verify_res": {
                        "execution": False,
                        "error": res,
                        "formal": None,
                        "manual": False
                    }
                }
        if tgt_dialect == 'pg' and 'ora2pg' not in exp_res and not 'snowflake' in [src_dialect, tgt_dialect]:
            res = ora2pg_tran(test_sql, src_dialect, tgt_dialect, get_all_db_name(tgt_dialect), number)
            res = post_process_for_reserved_keyword(res, src_dialect, tgt_dialect)
            exp_res['ora2pg'] = {
                "tran_res": res,
            }

        # res_distill = future_ds_llama_translate.result()
        # res_distill_processed = post_process_for_reserved_keyword(res_distill, src_dialect, tgt_dialect)
        # exp_res['DS-distill-70b'] = {
        #     "tran_res": res_distill_processed,
        # }
        res_v3 = future_ds_v3.result()
        res_v3 = post_process_for_reserved_keyword(res_v3, src_dialect, tgt_dialect)
        exp_res['Deepseek-v3'] = {
            "tran_res": res_v3
        }
        res_ds = future_translate.result()
        res_ds_processed = post_process_for_reserved_keyword(res_ds, src_dialect, tgt_dialect)
        exp_res['DS'] = {
            "tran_res": res_ds_processed,
        }
        # res_qwen3 = future_qwen3.result()
        # res_qwen3_processed = post_process_for_reserved_keyword(res_qwen3, src_dialect, tgt_dialect)
        # exp_res['Qwen3-30B'] = {
        #     "tran_res": res_qwen3_processed,
        # }
        # res_feedback = future_feedback.result()
        # res_feedback_processed = post_process_for_reserved_keyword(res_feedback, src_dialect, tgt_dialect)
        # exp_res['DS-Feedback'] = {
        #     "tran_res": res_feedback_processed
        # }
        if cracksql_flag:
            res_cracksql, lift_history = future_cracksql.result()
            if res_cracksql is not None:
                res_cracksql = post_process_for_reserved_keyword(res_cracksql, src_dialect, tgt_dialect)
                exp_res['cracksql'] = {
                    "tran_res": res_cracksql
                }
            else:
                exp_res['cracksql'] = {
                    "tran_res": "parsing error",
                    "verify_res": {
                        "execution": False,
                        "error": 'parsing error',
                        "formal": None,
                        "manual": False
                    }
                }
    return exp_res


def tran_multi_point_dataset(dataset_path: str, name, add_cracksql_flag, tran_flag=False):
    if dataset_path.endswith('exp_res.json') or dataset_path.endswith('exp_res_flt.json') or dataset_path.endswith(
            'dedup.json'):
        res_path = dataset_path
    else:
        res_path = f'{dataset_path.removesuffix(".json")}_exp_res.json'
    if os.path.exists(res_path):
        with open(res_path, 'r') as file:
            queries = json.load(file)
    else:
        with open(dataset_path, 'r') as file:
            queries = json.load(file)
    to_tran_queries = []
    for i in range(len(queries)):
        if 'exp_res' not in queries[i] or 'sqlines' not in queries[i]['exp_res']:
            to_tran_queries.append(queries[i])
    divide = 4
    slice_len = math.ceil(len(to_tran_queries) / divide)
    queries1 = to_tran_queries[:slice_len]
    queries2 = to_tran_queries[slice_len:2 * slice_len]
    queries3 = to_tran_queries[2 * slice_len:3 * slice_len]
    queries4 = to_tran_queries[3 * slice_len:]
    for i in tqdm(range(len(queries1))):
        with ThreadPoolExecutor(max_workers=divide) as executor:
            tran_flag1 = 'exp_res' not in queries1[i] or 'sqlines' not in queries1[i]['exp_res']
            tran_flag2 = len(queries2) > i and ('exp_res' not in queries2[i] or 'sqlines' not in queries2[i]['exp_res'])
            tran_flag3 = len(queries3) > i and ('exp_res' not in queries3[i] or 'sqlines' not in queries3[i]['exp_res'])
            tran_flag4 = len(queries4) > i and ('exp_res' not in queries4[i] or 'sqlines' not in queries4[i]['exp_res'])
            if tran_flag1:
                future1 = executor.submit(tran_multi_query, queries1[i], f'{name}1', add_cracksql_flag, tran_flag)
            if tran_flag2:
                future2 = executor.submit(tran_multi_query, queries2[i], f'{name}2', add_cracksql_flag, tran_flag)
            if tran_flag3:
                future3 = executor.submit(tran_multi_query, queries3[i], f'{name}3', add_cracksql_flag, tran_flag)
            if tran_flag4:
                future4 = executor.submit(tran_multi_query, queries4[i], f'{name}4', add_cracksql_flag, tran_flag)
            if tran_flag1:
                if 'cracksql' in queries1[i].get('exp_res', {}):
                    flag = True
                    cracksql_res = queries1[i]['exp_res']['cracksql']
                else:
                    flag = False
                queries1[i]['exp_res'] = future1.result()
                if flag and queries1[i]['exp_res'] is not None:
                    queries1[i]['exp_res']['cracksql'] = cracksql_res
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)
            if tran_flag2:
                if 'cracksql' in queries2[i].get('exp_res', {}):
                    flag = True
                    cracksql_res = queries2[i]['exp_res']['cracksql']
                else:
                    flag = False
                queries2[i]['exp_res'] = future2.result()
                if flag and queries2[i]['exp_res'] is not None:
                    queries2[i]['exp_res']['cracksql'] = cracksql_res
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)
            if tran_flag3:
                if 'cracksql' in queries3[i].get('exp_res', {}):
                    flag = True
                    cracksql_res = queries3[i]['exp_res']['cracksql']
                else:
                    flag = False
                queries3[i]['exp_res'] = future3.result()
                if flag and queries3[i]['exp_res'] is not None:
                    queries3[i]['exp_res']['cracksql'] = cracksql_res
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)
            if tran_flag4:
                if 'cracksql' in queries4[i].get('exp_res', {}):
                    flag = True
                    cracksql_res = queries4[i]['exp_res']['cracksql']
                else:
                    flag = False
                queries4[i]['exp_res'] = future4.result()
                if flag and queries4[i]['exp_res'] is not None:
                    queries4[i]['exp_res']['cracksql'] = cracksql_res
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)
            # with open(f'{dataset_path}_exp_res.json', 'w') as f:
            #     json.dump(all_res, f, indent=4)


def tran_model_bias(sql_dict, model_id, name):
    src_dialect = None
    tgt_dialect = None
    ans_sql = None
    for key, value in sql_dict.items():
        if src_dialect is None:
            src_dialect = key
        elif tgt_dialect is None:
            tgt_dialect = key
            ans_sql = value
    db_param = {
        src_dialect: {},
        tgt_dialect: {}
    }
    assert isinstance(sql_dict, dict)
    exp_res = sql_dict.get('exp_res', {})
    flag_res_sql, res_res_sql = sql_dependent_execute(tgt_dialect,
                                                      get_tran_db_name(get_all_db_name(tgt_dialect)),
                                                      ans_sql,
                                                      db_param[tgt_dialect])
    if not flag_res_sql:
        return None
    max_workers = 4
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_translate = executor.submit(model_translate, sql_dict, src_dialect, tgt_dialect,
                                           model_id,
                                           db_param, name)
        res_qwen3 = future_translate.result()
        res_qwen3_processed = post_process_for_reserved_keyword(res_qwen3, src_dialect, tgt_dialect)
        exp_res[model_id] = {
            "tran_res": res_qwen3_processed
        }
    return exp_res


def tran_model_dataset_bias(dataset_path: str, model_id, name):
    if dataset_path.endswith('exp_res.json') or dataset_path.endswith('exp_res_flt.json') or dataset_path.endswith(
            'dedup.json'):
        res_path = dataset_path
    else:
        res_path = f'{dataset_path.removesuffix(".json")}_exp_res.json'
    if os.path.exists(res_path):
        with open(res_path, 'r') as file:
            queries = json.load(file)
    else:
        with open(dataset_path, 'r') as file:
            queries = json.load(file)
    to_tran_queries = []
    for i in range(len(queries)):
        if 'exp_res' not in queries[i] or model_id not in queries[i]['exp_res']:
            to_tran_queries.append(queries[i])
    divide = 4
    slice_len = math.ceil(len(to_tran_queries) / divide)
    queries1 = to_tran_queries[:slice_len]
    queries2 = to_tran_queries[slice_len:2 * slice_len]
    queries3 = to_tran_queries[2 * slice_len:3 * slice_len]
    queries4 = to_tran_queries[3 * slice_len:]
    for i in tqdm(range(len(queries1))):
        with ThreadPoolExecutor(max_workers=divide) as executor:
            tran_flag1 = 'exp_res' not in queries1[i] or model_id not in queries1[i]['exp_res']
            tran_flag2 = len(queries2) > i and ('exp_res' not in queries2[i] or model_id not in queries1[i]['exp_res'])
            tran_flag3 = len(queries3) > i and ('exp_res' not in queries3[i] or model_id not in queries1[i]['exp_res'])
            tran_flag4 = len(queries4) > i and ('exp_res' not in queries4[i] or model_id not in queries1[i]['exp_res'])
            if tran_flag1:
                future1 = executor.submit(tran_model_bias, queries1[i], model_id, f'{model_id}_{name}_1')
            if tran_flag2:
                future2 = executor.submit(tran_model_bias, queries2[i], model_id, f'{model_id}_{name}_2')
            if tran_flag3:
                future3 = executor.submit(tran_model_bias, queries3[i], model_id, f'{model_id}_{name}_3')
            if tran_flag4:
                future4 = executor.submit(tran_model_bias, queries4[i], model_id, f'{model_id}_{name}_4')
            if tran_flag1:
                queries1[i]['exp_res'] = future1.result()
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)
            if tran_flag2:
                queries2[i]['exp_res'] = future2.result()
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)
            if tran_flag3:
                queries3[i]['exp_res'] = future3.result()
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)
            if tran_flag4:
                queries4[i]['exp_res'] = future4.result()
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)


def tran_model(sql_dict, model_id, name):
    if 'Dialect' in sql_dict:
        src_dialect = sql_dict['Dialect']['Src']
        tgt_dialect = sql_dict['Dialect']['Tgt']
        ans_sql = sql_dict['SQL'][tgt_dialect]
    else:
        src_dialect = None
        tgt_dialect = None
        ans_sql = None
        for key, value in sql_dict.items():
            if src_dialect is None:
                src_dialect = key
            elif tgt_dialect is None:
                tgt_dialect = key
                ans_sql = value
    db_param = {
        src_dialect: {},
        tgt_dialect: {}
    }
    assert isinstance(sql_dict, dict)
    exp_res = sql_dict.get('exp_res', {})
    if 'SQL' in exp_res:
        points = exp_res['SQL']['points']
    else:
        points = sql_dict.get('points', {})
    for point1 in points:
        if isinstance(point1, dict):
            p_name = point1['point']
        else:
            p_name = point1
        point_db_param = fetch_db_param(p_name, src_dialect, tgt_dialect)
        for key, value in point_db_param.items():
            db_param[key].update(value)
    flag_res_sql, res_res_sql = sql_dependent_execute(tgt_dialect,
                                                      get_all_db_name(tgt_dialect),
                                                      ans_sql,
                                                      db_param[tgt_dialect])
    if not flag_res_sql:
        return exp_res
    max_workers = 4
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        if 'SQL' in sql_dict:
            sql_dict = sql_dict['SQL']
        future_translate = executor.submit(model_translate, sql_dict, src_dialect, tgt_dialect,
                                           model_id,
                                           db_param, name)
        res_qwen3 = future_translate.result()
        res_qwen3_processed = post_process_for_reserved_keyword(res_qwen3, src_dialect, tgt_dialect)
        exp_res[model_id] = {
            "tran_res": res_qwen3_processed
        }
    return exp_res


def tran_model_dataset(dataset_path: str, model_id, name):
    if dataset_path.endswith('exp_res.json') or dataset_path.endswith('exp_res_flt.json') or dataset_path.endswith(
            'dedup.json'):
        res_path = dataset_path
    else:
        res_path = f'{dataset_path.removesuffix(".json")}_exp_res.json'
    if os.path.exists(res_path):
        with open(res_path, 'r') as file:
            queries = json.load(file)
    else:
        with open(dataset_path, 'r') as file:
            queries = json.load(file)
    to_tran_queries = []
    for i in range(len(queries)):
        if 'exp_res' not in queries[i] or (queries[i]['exp_res'] is not None and model_id not in queries[i]['exp_res']):
            to_tran_queries.append(queries[i])
    divide = 4
    slice_len = math.ceil(len(to_tran_queries) / divide)
    queries1 = to_tran_queries[:slice_len]
    queries2 = to_tran_queries[slice_len:2 * slice_len]
    queries3 = to_tran_queries[2 * slice_len:3 * slice_len]
    queries4 = to_tran_queries[3 * slice_len:]
    for i in tqdm(range(len(queries1))):
        with ThreadPoolExecutor(max_workers=divide) as executor:
            tran_flag1 = 'exp_res' not in queries1[i] or model_id not in queries1[i]['exp_res']
            tran_flag2 = len(queries2) > i and ('exp_res' not in queries2[i] or model_id not in queries1[i]['exp_res'])
            tran_flag3 = len(queries3) > i and ('exp_res' not in queries3[i] or model_id not in queries1[i]['exp_res'])
            tran_flag4 = len(queries4) > i and ('exp_res' not in queries4[i] or model_id not in queries1[i]['exp_res'])
            if tran_flag1:
                future1 = executor.submit(tran_model, queries1[i], model_id, f'{model_id}_{name}_1')
            if tran_flag2:
                future2 = executor.submit(tran_model, queries2[i], model_id, f'{model_id}_{name}_2')
            if tran_flag3:
                future3 = executor.submit(tran_model, queries3[i], model_id, f'{model_id}_{name}_3')
            if tran_flag4:
                future4 = executor.submit(tran_model, queries4[i], model_id, f'{model_id}_{name}_4')
            if tran_flag1:
                queries1[i]['exp_res'] = future1.result()
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)
            if tran_flag2:
                queries2[i]['exp_res'] = future2.result()
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)
            if tran_flag3:
                queries3[i]['exp_res'] = future3.result()
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)
            if tran_flag4:
                queries4[i]['exp_res'] = future4.result()
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)


def tran_dspy(sql_dict, name):
    if 'Dialect' in sql_dict:
        src_dialect = sql_dict['Dialect']['Src']
        tgt_dialect = sql_dict['Dialect']['Tgt']
        ans_sql = sql_dict['SQL'][tgt_dialect]
    else:
        src_dialect = None
        tgt_dialect = None
        ans_sql = None
        for key, value in sql_dict.items():
            if src_dialect is None:
                src_dialect = key
            elif tgt_dialect is None:
                tgt_dialect = key
                ans_sql = value
    db_param = {
        src_dialect: {},
        tgt_dialect: {}
    }
    assert isinstance(sql_dict, dict)
    exp_res = sql_dict.get('exp_res', {})
    if 'SQL' in exp_res:
        points = exp_res['SQL']['points']
    else:
        points = sql_dict.get('points', {})
    for point1 in points:
        if isinstance(point1, dict):
            p_name = point1['point']
        else:
            p_name = point1
        point_db_param = fetch_db_param(p_name, src_dialect, tgt_dialect)
        for key, value in point_db_param.items():
            db_param[key].update(value)
    flag_res_sql, res_res_sql = sql_dependent_execute(tgt_dialect,
                                                      get_all_db_name(tgt_dialect),
                                                      ans_sql,
                                                      db_param[tgt_dialect])
    if not flag_res_sql:
        return None
    max_workers = 4
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        future_translate = executor.submit(translate_dspy, sql_dict, src_dialect, tgt_dialect,
                                           db_param, name)
        res_dspy = future_translate.result()
        res_dspy_processed = post_process_for_reserved_keyword(res_dspy, src_dialect, tgt_dialect)
        exp_res['dspy'] = {
            "tran_res": res_dspy_processed,
        }
    return exp_res


def tran_dspy_dataset(dataset_path: str, name):
    if dataset_path.endswith('exp_res.json') or dataset_path.endswith('exp_res_flt.json') or dataset_path.endswith(
            'dedup.json'):
        res_path = dataset_path
    else:
        res_path = f'{dataset_path.removesuffix(".json")}_exp_res.json'
    if os.path.exists(res_path):
        with open(res_path, 'r') as file:
            queries = json.load(file)
    else:
        with open(dataset_path, 'r') as file:
            queries = json.load(file)
    to_tran_queries = []
    for i in range(len(queries)):
        if 'exp_res' not in queries[i] or (queries[i]['exp_res'] is not None and 'dspy' not in queries[i]['exp_res']):
            to_tran_queries.append(queries[i])
    divide = 4
    slice_len = math.ceil(len(to_tran_queries) / divide)
    queries1 = to_tran_queries[:slice_len]
    queries2 = to_tran_queries[slice_len:2 * slice_len]
    queries3 = to_tran_queries[2 * slice_len:3 * slice_len]
    queries4 = to_tran_queries[3 * slice_len:]
    for i in tqdm(range(len(queries1))):
        with ThreadPoolExecutor(max_workers=divide) as executor:
            tran_flag1 = 'exp_res' not in queries1[i] or 'dspy' not in queries1[i]['exp_res']
            tran_flag2 = len(queries2) > i and ('exp_res' not in queries2[i] or 'dspy' not in queries1[i]['exp_res'])
            tran_flag3 = len(queries3) > i and ('exp_res' not in queries3[i] or 'dspy' not in queries1[i]['exp_res'])
            tran_flag4 = len(queries4) > i and ('exp_res' not in queries4[i] or 'dspy' not in queries1[i]['exp_res'])
            if tran_flag1:
                future1 = executor.submit(tran_dspy, queries1[i], f'dspy_{name}_1')
            if tran_flag2:
                future2 = executor.submit(tran_dspy, queries2[i], f'dspy_{name}_2')
            if tran_flag3:
                future3 = executor.submit(tran_dspy, queries3[i], f'dspy_{name}_3')
            if tran_flag4:
                future4 = executor.submit(tran_dspy, queries4[i], f'dspy_{name}_4')
            if tran_flag1:
                queries1[i]['exp_res'] = future1.result()
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)
            if tran_flag2:
                queries2[i]['exp_res'] = future2.result()
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)
            if tran_flag3:
                queries3[i]['exp_res'] = future3.result()
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)
            if tran_flag4:
                queries4[i]['exp_res'] = future4.result()
                with open(res_path, 'w') as f:
                    json.dump(queries, f, indent=4)
