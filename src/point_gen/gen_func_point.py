# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: gen_func_point.py$
# @Author: 10379
# @Time: 2024/12/29 11:41
import json
import os.path

from tqdm import tqdm

from model.model_init import init_model
from point_gen.prompt import sys_prompt_bat, user_prompt_bat
import chromadb
from chromadb.utils import embedding_functions

from utils.tools import get_proj_root_path


def gen_func_point(func_name, func_desc, src_dialect, tgt_dialect):
    system_prompt = sys_prompt_bat.format(src_dialect=src_dialect, tgt_dialect=tgt_dialect)
    prompt = user_prompt_bat.format(src_dialect=src_dialect, tgt_dialect=tgt_dialect, function_name=func_name,
                                    description=func_desc)

    print(system_prompt)
    print(prompt)


print(get_proj_root_path())


def load_function_name_desc_pair_list(dialect: str):
    if dialect == 'mysql':
        file_path = f'{str(get_proj_root_path())}/src/point_gen/mysql_8_kb.json'
    elif dialect == 'oracle':
        file_path = f'{str(get_proj_root_path())}/src/point_gen/oracle_11_kb.json'
    elif dialect == 'pg':
        file_path = f'{str(get_proj_root_path())}/src/point_gen/pg_14_kb.json'
    else:
        assert False
    with open(file_path, 'r') as json_file:
        data = json.load(json_file)
    res = []
    for item in data:
        if item['type'] == 'function':
            res.append({
                "name": item['keyword'],
                "desc": item['description']
            })
    return res


def func_to_text(name, desc):
    return f"Function Name: {name}\nFunction Description: {desc}"


def get_model_id(model_name: str):
    if 'qwen' in model_name:
        return 'qwen'
    else:
        return 'cracksql'


def fetch_similar_tgt_function(src_func_name, src_func_desc: str,
                               tgt_dialect: str, embedding_model: str, top_k: int = 5):
    chroma_db_path = f'{str(get_proj_root_path())}/src/point_gen/chroma_store'
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

    if collection.count() == 0:
        target_functions = load_function_name_desc_pair_list(tgt_dialect)
        BATCH = 100
        for start in tqdm(range(0, len(target_functions), BATCH)):
            batch = target_functions[start:start + BATCH]
            ids = [f"t_{start + i}" for i in range(len(batch))]
            docs = [func_to_text(t["name"], t["desc"]) for t in batch]
            metas = [{"name": t["name"], "desc": t["desc"]} for t in batch]
            collection.add(ids=ids, documents=docs, metadatas=metas)

    search_cont = func_to_text(src_func_name, src_func_desc)

    results = collection.query(
        query_texts=[search_cont],
        n_results=top_k,
        include=["metadatas", "documents", "distances"]
    )

    out = []
    for meta, dist, doc in zip(results["metadatas"][0],
                               results["distances"][0],
                               results["documents"][0]):
        out.append({
            "name": meta.get("name"),
            "desc": meta.get("desc"),
            "distance": dist,
            "doc": doc,
        })
    return out


def before_first_paren(s: str) -> str:
    return s.split("(", 1)[0]


def function_point_generation(src_dialect, tgt_dialect, embedding_model):
    src_dialect_name_pair_list = load_function_name_desc_pair_list(src_dialect)
    tgt_dialect_name_pair_list = load_function_name_desc_pair_list(tgt_dialect)
    out_path = f'{str(get_proj_root_path())}/src/point_gen/{src_dialect}_{tgt_dialect}_{get_model_id(embedding_model)}.json'
    if os.path.exists(out_path):
        with open(out_path, 'r') as f:
            res = json.load(f)
    else:
        res = []
    for src_dialect_name_pair in tqdm(src_dialect_name_pair_list):
        name = src_dialect_name_pair['name']
        flag = False
        for res_item in res:
            if res_item['name'] == name:
                flag = True
                break
        if flag:
            continue
        desc = src_dialect_name_pair['desc']
        flag = False
        for tgt_dialect_name_pair in tgt_dialect_name_pair_list:
            if before_first_paren(name.lower()) == before_first_paren(tgt_dialect_name_pair['name'].lower()):
                flag = True
        if not flag:
            system_prompt = sys_prompt_bat.format(src_dialect=src_dialect, tgt_dialect=tgt_dialect)
            ref_func_descriptions = ''
            for item in fetch_similar_tgt_function(name, desc, tgt_dialect, embedding_model, top_k=5):
                ref_func_descriptions += f"  - {item['name']}: `{item['desc']}`\n"
            prompt = user_prompt_bat.format(src_dialect=src_dialect, tgt_dialect=tgt_dialect, function_name=name,
                                            description=desc, ref_func_descriptions=ref_func_descriptions)
            model = init_model('DeepSeek-V3.2')
            ans = model.trans_func([], system_prompt, prompt)
            res.append({
                "name": name,
                "desc": desc,
                "model_ans": ans
            })
            with open(out_path, 'w') as f:
                json.dump(res, f, indent=4)


from concurrent.futures import ThreadPoolExecutor, as_completed


def run_one(src_dialect, tgt_dialect, model_path):
    return function_point_generation(src_dialect, tgt_dialect, model_path)


def gen_cracksql():
    dialects = ['mysql', 'pg', 'oracle']
    model_path = f'{str(get_proj_root_path())}/src/transpiler/cracksql_driver/all-MiniLM-L6-v2'

    tasks = []
    for src in dialects:
        for tgt in dialects:
            if src == tgt:
                continue
            tasks.append((src, tgt))
    max_workers = min(32, len(tasks))

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(run_one, src, tgt, model_path) for src, tgt in tasks]

        for fut in as_completed(futures):
            try:
                fut.result()  # 如果函数内部抛异常，这里会抛出来
            except Exception as e:
                print("Task Failed：", e)


def gen_qwen():
    dialects = ['mysql', 'pg', 'oracle']
    model_path = f'Your qwen embedding model path'

    tasks = []
    for src in dialects:
        for tgt in dialects:
            if src == tgt:
                continue
            tasks.append((src, tgt))
    max_workers = min(32, len(tasks))

    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = [ex.submit(run_one, src, tgt, model_path) for src, tgt in tasks]

        for fut in as_completed(futures):
            try:
                fut.result()  # 如果函数内部抛异常，这里会抛出来
            except Exception as e:
                print("Task Failed：", e)
