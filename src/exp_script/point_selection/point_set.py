import os

from tqdm import tqdm

from exp_script.gen_dataset_for_multi_points import load_points_path
from sql_gen.generator.generate_pipeline import generate_equivalent_sql_pair
from sql_gen.generator.method import add_point_to_point_dict

import json
import random


def generate_same_scale_with_sample(sql_number, output_path, points_path):
    all_weight = [572, 100, 200, 300, 400, 500]
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
    while k < len(all_weight):
        while i < regions[k]:
            weights = [116, 67, 102, 107, 110, 70]
            dialects = {
                1: {"Src": "mysql", "Tgt": "pg"},
                2: {"Src": "mysql", "Tgt": "oracle"},
                3: {"Src": "pg", "Tgt": "mysql"},
                4: {"Src": "pg", "Tgt": "oracle"},
                5: {"Src": "oracle", "Tgt": "mysql"},
                6: {"Src": "oracle", "Tgt": "pg"}
            }
            keys = list(dialects.keys())
            selected_key = random.choices(keys, weights=weights, k=1)[0]
            dialect_pair_id = dialects[selected_key]
            attempts = 0
            while attempts < 50:
                attempts += 1
                src_dialect = dialect_pair_id['Src']
                tgt_dialect = dialect_pair_id['Tgt']
                points = load_points_path(src_dialect, tgt_dialect, points_path)
                point_num = random.randint(points_lower_bound[k], points_upper_bound[k])
                point_list = []
                for j in range(point_num):
                    new_point = random.sample(points, 1)[0]
                    point_list.append(new_point)
                added_points = []
                for p in point_list:
                    add_point_to_point_dict(added_points, p)
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
                pbar.set_postfix({"dialect": f"{dialect_pair_id['Src']}->{dialect_pair_id['Tgt']}", "range": k})
                with open(output_path, 'w') as file:
                    json.dump(res, file, indent=4)
        k += 1
