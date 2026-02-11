# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: gen_dataset_for_length$
# @Author: 10379
# @Time: 2025/7/29 13:56
import json
import os.path
import random
from collections import Counter

from tqdm import tqdm

from antlr_parser.Tree import TreeNode
from antlr_parser.parse_tree import parse_tree
from exp_script.check_no_res import make_hashable
from exp_script.statistic_file import statistic
from exp_script.translate_scripts.tran_dataset import tran_dataset
from exp_script.verify_res import process_file
from sql_gen.generator.add_irrelevant_components import add_irrelevant_sql
from sql_gen.generator.point_loader import load_point_by_name
from sql_gen.generator.token_statistic import stat_tokens
from utils.ExecutionEnv import ExecutionEnv
from utils.db_connector import sql_execute
from utils.tools import get_all_db_name, get_proj_root_path


def stat_token_query(sql: str, src_dialect: str):
    tree_node, _, _, _ = parse_tree(sql, src_dialect)
    if tree_node is None:
        return None
    tree_node = TreeNode.make_g4_tree_by_node(tree_node, src_dialect)
    return stat_tokens(tree_node)


def get_add_dataset(tokens: int):
    sqls = []
    out_path = os.path.join(get_proj_root_path(), 'exp_data', 'sql_len', f'sql_len_add_{tokens}.json')
    with open(os.path.join(get_proj_root_path(), 'exp_data', 'sql_len', 'sql_len_add_0.json')) as f:
        sqls = json.load(f)
    if os.path.exists(out_path):
        with open(out_path, 'r', encoding='utf-8') as f:
            cur_set = json.load(f)
    else:
        cur_set = []
    for sql in tqdm(sqls):
        max_retry = 50
        i = 0
        while i < max_retry:
            try:
                sql = add_irrelevant_sql(sql['SQL'], sql['Dialect']['Src'], sql['Dialect']['Tgt'], tokens)
                cur_set.append(sql)
                sql['SQL'] = sql
                break
            except Exception as e:
                i += 1
                continue
        assert isinstance(sql, dict)
        sql.pop('exp_res')
        cur_set.append(sql)
    with open(out_path, 'w', encoding='utf-8') as f:
        json.dump(cur_set, f, indent=4)
    return out_path


output_path1 = get_add_dataset(100)
output_path2 = get_add_dataset(200)
output_path3 = get_add_dataset(300)
output_path4 = get_add_dataset(400)

tran_dataset(output_path1, 'sql_len1', True)
tran_dataset(output_path2, 'sql_len2', True)
tran_dataset(output_path3, 'sql_len3', True)
tran_dataset(output_path4, 'sql_len4', True)

def get_res_path(path: str):
    if path.endswith('exp_res.json') or path.endswith('exp_res_flt.json'):
        res_path = path
    else:
        res_path = f'{path.removesuffix(".json")}_exp_res.json'
    return res_path

res_path1 = get_res_path(output_path1)
res_path2 = get_res_path(output_path2)
res_path3 = get_res_path(output_path3)
res_path4 = get_res_path(output_path4)

process_file(res_path1, multi_mode=True, tran_flag=False)
process_file(res_path2, multi_mode=True, tran_flag=False)
process_file(res_path3, multi_mode=True, tran_flag=False)
process_file(res_path4, multi_mode=True, tran_flag=False)

statistic(res_path1)
statistic(res_path2)
statistic(res_path3)
statistic(res_path4)

