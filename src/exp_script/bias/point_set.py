import os
import traceback

from func_timeout import func_timeout, FunctionTimedOut
from tqdm import tqdm

from antlr_parser.general_tree_analysis import fetch_all_table_in_sql
from exp_script.re_struct_sqls import src_dialect
from sql_gen.generator.element.Point import Point
from sql_gen.generator.generate_pipeline import load_no_point_sqls, generate_sql_by_points
from sql_gen.generator.method import add_point_to_point_dict

import json
import random

from sql_gen.generator.point_parser import parse_point
from sql_gen.generator.point_type.TranPointType import ReservedKeywordType
from sql_gen.generator.reference_sql_selection import minimal_set_selection
from sql_gen.generator.rewriter import rewrite_sql
from utils.ExecutionEnv import ExecutionEnv
from utils.db_connector import sql_execute
from utils.tools import get_all_db_name


def load_all_points(points_path):
    with open(points_path, 'r') as file:
        points_categories = json.load(file)
    points_list = []
    for key, value in points_categories.items():
        points_list = points_list + value
    return points_categories, points_list


def add_point_to_point_dict(points1: list[dict], point):
    flag = False
    if isinstance(point, str):
        point_name = point
    elif isinstance(point, dict):
        point_name = point['Desc']
    else:
        point_name = point.point_name
    for exist_point in points1:
        if exist_point['point']['Desc'] == point_name:
            exist_point['num'] = exist_point['num'] + 1
            flag = True
    if not flag:
        points1.append({
            "point": point,
            "num": 1
        })


def gen_bias_only_10(sql_number, output_path, points_path):
    all_weight = [600, 100, 200, 300, 400, 500]
    k = 0
    regions = []
    cur_weight = 0
    for i in range(len(all_weight)):
        cur_weight += all_weight[i]
        regions.append(cur_weight / sum(all_weight) * sql_number)
    i = 0
    res = []
    if os.path.exists(output_path):
        with open(output_path, 'r') as file:
            res = json.load(file)
            i = len(res)
            while k < len(regions) and i >= regions[k]:
                k += 1
    pbar = tqdm(total=sql_number, desc="Generating SQL Pairs", unit="pair")
    pbar.update(len(res))
    points_categories, points_list = load_all_points(points_path)

    while i < sql_number:
        first_point = random.sample(points_list, 1)[0]
        src_dialect = first_point['point']['Dialect']['Src']
        tgt_dialect = first_point['point']['Dialect']['Tgt']
        attempts = 0
        while attempts < 50:
            attempts += 1
            points = points_categories[f'{src_dialect}_{tgt_dialect}']
            point_num = random.randint(8, 9)
            point_list = []
            for j in range(point_num):
                new_point = random.sample(points, 1)[0]
                point_list.append(new_point)
            added_points = []
            for p in point_list:
                add_point_to_point_dict(added_points, p['point'])
            print(added_points)
            sql_pair = generate_equivalent_sql_pair(src_dialect, tgt_dialect, added_points, only_cur_sql_mode=True)
            if sql_pair is not None:
                num = 0
                for p in sql_pair['points']:
                    num += p['num']
                if not num < 10:
                    break
        if sql_pair is not None:
            print(sql_pair)
            res.append(sql_pair)
            i += 1
            pbar.update(1)
            pbar.set_postfix({"dialect": f"{src_dialect}->{tgt_dialect}", "range": k})
            with open(output_path, 'w') as file:
                json.dump(res, file, indent=4)
    k += 1


def generate_same_number_with_sample(sql_number, output_path, points_path):
    all_weight = [661, 100, 200, 300, 400, 500]
    k = 0
    points_lower_bound = [1, 2, 4, 6, 8, 10]
    points_upper_bound = [1, 3, 5, 7, 9, 11]
    regions = []
    cur_weight = 0
    for i in range(len(all_weight)):
        cur_weight += all_weight[i]
        regions.append(cur_weight / sum(all_weight) * sql_number)
    i = 0
    res = []
    if os.path.exists(output_path):
        with open(output_path, 'r') as file:
            res = json.load(file)
            i = len(res)
            while k < len(regions) and i >= regions[k]:
                k += 1
    pbar = tqdm(total=sql_number, desc="Generating SQL Pairs", unit="pair")
    pbar.update(len(res))
    points_categories, points_list = load_all_points(points_path)

    while k < len(all_weight):
        while i < regions[k]:
            first_point = random.sample(points_list, 1)[0]
            src_dialect = first_point['point']['Dialect']['Src']
            tgt_dialect = first_point['point']['Dialect']['Tgt']
            attempts = 0
            while attempts < 50:
                attempts += 1
                points = points_categories[f'{src_dialect}_{tgt_dialect}']
                point_num = random.randint(points_lower_bound[k], points_upper_bound[k])
                point_list = []
                for j in range(point_num):
                    new_point = random.sample(points, 1)[0]
                    point_list.append(new_point)
                added_points = []
                for p in point_list:
                    add_point_to_point_dict(added_points, p['point'])
                print(added_points)
                sql_pair = generate_equivalent_sql_pair(src_dialect, tgt_dialect, added_points, only_cur_sql_mode=True)
                if sql_pair is not None:
                    num = 0
                    for p in sql_pair['points']:
                        num += p['num']
                    if not num < points_lower_bound[k]:
                        break
            if sql_pair is not None:
                print(sql_pair)
                res.append(sql_pair)
                i += 1
                pbar.update(1)
                pbar.set_postfix({"dialect": f"{src_dialect}->{tgt_dialect}", "range": k})
                with open(output_path, 'w') as file:
                    json.dump(res, file, indent=4)
        k += 1


def generate_equivalent_sql_pair(src_dialect: str, tgt_dialect: str, point_def_list: list[dict], max_retry_time=5,
                                 only_cur_sql_mode=False) -> dict | None:
    """
    Generates an equivalent SQL pair based on the given point and configuration parameters.
    Parameters:
    :param point: str give the point_id that used to generate.
    :param src_dialect: str - The source dialect of the SQL pair.
    :param tgt_dialect: str - The target dialect of the SQL pair.
    :param max_retry_time: int - The maximum number retrying time
    Returns:
    :return: dict - A dict containing two equivalent SQL expressions that are logically equivalent and the environment.
            Example: {'mysql': '...', 'oracle': '...', 'points': {...}}
    """
    i = 0
    while True:
        i += 1
        if i > max_retry_time:
            print(f'No SQL pair can be generated with point requirement {point_def_list}')
            return None
        print(f'round {i}')
        point_req_list = []
        for point_req in point_def_list:
            print(point_req['point'])
            point_req_list.append({
                "point": parse_point(point_req['point']),
                "num": point_req['num']
            })
        if i < 0.6 * max_retry_time:
            aggressive_flag = True
        else:
            aggressive_flag = False
        try:
            execution_env = ExecutionEnv(src_dialect, get_all_db_name(src_dialect))
            for point_req in point_req_list:
                point = point_req['point']
                assert isinstance(point, Point)
                if point.tag is not None and 'DB PARAMETER' in point.tag:
                    for key, value in point.tag['DB PARAMETER'].items():
                        flag = execution_env.add_param(key, value)
                        if not flag:
                            raise ValueError('DB Parameter conflict')
            for point in point_def_list:
                point = point['point']
                if point is None:
                    continue
                if "Tag" in point:
                    param = point['Tag']
                    if 'DB PARAMETER' in param:
                        for key, value in param['DB PARAMETER'].items():
                            flag = execution_env.add_param(key, value)
                            if not flag:
                                raise ValueError('DB Parameter conflict')
            sqls = load_no_point_sqls(src_dialect, tgt_dialect)
            random.shuffle(sqls)
            sqls = load_no_point_sqls(src_dialect, tgt_dialect)
            random.shuffle(sqls)
            minimal_sql_sets = minimal_set_selection(sqls, point_req_list)
            sql_pair = generate_sql_by_points(point_req_list, aggressive_flag, execution_env, [], None,
                                              only_cur_sql_mode)
            if sql_pair is None:
                continue
            complete_flag = True
            for point_req in point_req_list:
                flag = False
                for p in sql_pair['points']:
                    if p['point'] == point_req['point'] and p['num'] == point_req['num']:
                        flag = True
                if not flag:
                    complete_flag = False
                    break
            if not complete_flag:
                continue
            parsed_points = []
            for point in point_def_list:
                parsed_point = parse_point(point['point'])
                if isinstance(parsed_point.point_type, ReservedKeywordType):
                    continue
                parsed_points.append(parsed_point)
            print(f'generated {src_dialect} SQL: ' + sql_pair[src_dialect])
            tgt_sql, all_rewrite_token, rewrite_points = rewrite_sql(src_dialect, tgt_dialect, sql_pair[src_dialect],
                                                                     parsed_points)

            if tgt_sql is None:
                print(f"\033[91m{tgt_sql}\033[0m")
                continue
            sql_pair[tgt_dialect] = tgt_sql
        except Exception as e:
            print(e)
            traceback.print_exc()
            continue
            # raise e
        db_param = {src_dialect: {}, tgt_dialect: {}}
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
            print(flag1)
            print(flag2)
        except FunctionTimedOut:
            print("SQL 执行超时（超过1分钟），跳过当前循环")
            continue
        except Exception as e:
            raise e
            print(f"执行出错: {e}")
            continue
        sql_pair['rewrite_tokens'] = all_rewrite_token
        src_tables = fetch_all_table_in_sql(sql_pair[src_dialect], src_dialect)
        tgt_tables = fetch_all_table_in_sql(sql_pair[tgt_dialect], tgt_dialect)
        if src_tables is None and tgt_tables is None:
            continue
        elif src_tables is None:
            sql_pair['tables'] = list(tgt_tables)
        elif tgt_tables is None:
            sql_pair['tables'] = list(src_tables)
        else:
            print(src_tables)
            print(tgt_tables)
            assert src_tables == tgt_tables
            sql_pair['tables'] = list(src_tables)
        if flag1 and flag2:
            break
        if sql_pair is None:
            raise ValueError('No SQL pair can be generated')
    return sql_pair
