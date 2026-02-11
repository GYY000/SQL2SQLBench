import json
import os.path
import random

from exp_script.generate_scripts.gen_dataset_same_scale_with_all import generate_same_scale_with_all
from exp_script.statistic_file import statistic
from exp_script.translate_scripts.tran_dataset import tran_dataset
from exp_script.verify_res import process_file
from utils.tools import get_proj_root_path

set_number = 150

output_path1 = 'your path to store sql sets1'
generate_same_scale_with_all(set_number, output_path1)

output_path2 = 'your path to store sql sets2'
generate_same_scale_with_all(set_number, output_path2)

tran_dataset(output_path1, 'determine_1', True)
tran_dataset(output_path2, 'determine_2', True)

# verify result
if output_path1.endswith('exp_res.json') or output_path1.endswith('exp_res_flt.json'):
    res1_path = output_path1
else:
    res1_path = f'{output_path1.removesuffix(".json")}_exp_res.json'

process_file(res1_path, multi_mode=True, tran_flag=False)

if output_path2.endswith('exp_res.json') or output_path2.endswith('exp_res_flt.json'):
    res2_path = output_path2
else:
    res2_path = f'{output_path2.removesuffix(".json")}_exp_res.json'

process_file(res2_path, multi_mode=True, tran_flag=False)


def sample_sqls_from_overall_dataset():
    all_data = []
    with open(os.path.join(get_proj_root_path(), 'exp_data', 'individual', 'individual.json'), 'r') as f:
        data = json.load(f)
        all_data += data

    for i in [2, 4, 6, 8, 10]:
        with open(os.path.join(get_proj_root_path(), 'exp_data', 'multi_point', f'sql_points_{i}_{i + 1}.json'),
                  'r') as f:
            data = json.load(f)
            all_data += data
    global set_number
    return random.sample(all_data, set_number)


statistic(res1_path)
statistic(res2_path)
statistic(None, sample_sqls_from_overall_dataset())
