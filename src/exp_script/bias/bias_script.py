from exp_script.bias.point_set import generate_same_number_with_sample
from exp_script.statistic_file import statistic
from exp_script.verify_res import process_file

sql_number = 100

tool = 'qwen'
output_path1 = f'your path to store sql sets1'
points1_path = f'your path to qwen_points.json'
generate_same_number_with_sample(sql_number, output_path1, points1_path)

output_path2 = f'your path to store sql sets2'
points2_path = f'your path to cracksql_points.json'
generate_same_number_with_sample(sql_number, output_path2, points2_path)

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

statistic(res1_path)
statistic(res2_path)
