from exp_script.point_selection.point_set import generate_same_scale_with_sample
from exp_script.translate_scripts.tran_dataset import tran_dataset
from exp_script.verify_res import process_file

sql_number = 100

out1_path = 'your path to store the final result sqls of set1 to be translated'
points1_path = 'your point path to sample1.json'
generate_same_scale_with_sample(sql_number, out1_path, points1_path)


out2_path = 'your path to store the final result sqls of set2 to be translated'
points2_path = 'your point path to sample2.json'
generate_same_scale_with_sample(sql_number, out2_path, points2_path)


tran_dataset(out1_path, 'sample_points1', True)
tran_dataset(out2_path, 'sample_points2', True)

# verify result
if out1_path.endswith('exp_res.json') or out1_path.endswith('exp_res_flt.json'):
    res1_path = out1_path
else:
    res1_path = f'{out1_path.removesuffix(".json")}_exp_res.json'

process_file(res1_path, multi_mode=True, tran_flag=False)


if out2_path.endswith('exp_res.json') or out2_path.endswith('exp_res_flt.json'):
    res2_path = out2_path
else:
    res2_path = f'{out2_path.removesuffix(".json")}_exp_res.json'

process_file(res2_path, multi_mode=True, tran_flag=False)
