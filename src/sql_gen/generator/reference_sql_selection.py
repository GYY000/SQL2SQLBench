import json
import os.path
from sql_gen.generator.element.Point import Point
from utils.tools import get_db_ids, get_schema_path, get_data_path


def fetch_foreign_key_tables(table):
    for db_id in get_db_ids():
        with open(os.path.join(get_schema_path(db_id), 'schema.json'), 'r') as file:
            schema = json.load(file)
        if table not in schema:
            continue
        fks = schema[table]['foreign_key']
        fk_tables = [fk['ref_table'] for fk in fks]
        for key, value in schema.items():
            if key == table:
                continue
            for fk in value['foreign_key']:
                if fk['ref_table'] == table:
                    fk_tables.append(key)
        return list(set(fk_tables))
    return []


def fetch_fk_tables(tables: list, all_schemas):
    fk_tables = set()
    for key, schema in all_schemas.items():
        for table in tables:
            if table in schema:
                for fk in schema[table]['foreign_key']:
                    if fk['ref_table'] not in tables and fk['ref_table'] not in fk_tables:
                        fk_tables.add(fk['ref_table'])
    return fk_tables


def relevant_sql_sort(used_tables: list, sqls: list[dict], added_points, all_schemas):
    sorted_sqls = []
    fk_tables = fetch_fk_tables(used_tables, all_schemas)
    for sql in sqls:
        cnt_used_tables = 0
        cnt_fk_tables = 0
        for tbl in sql['tables']:
            if tbl in used_tables:
                cnt_used_tables += 1
            elif tbl in fk_tables:
                cnt_fk_tables += 1
        sorted_sqls.append({
            'sql': sql,
            "cnt_used_tables": cnt_used_tables,
            "cnt_fk_tables": cnt_fk_tables,
            "cnt_points": len([p for p in sql['fulfilling_points'] if p not in added_points])
        })
    sorted_sqls.sort(
        key=lambda x: (x["cnt_points"], x["cnt_used_tables"], x["cnt_fk_tables"]),
        reverse=True
    )
    return [x["sql"] for x in sorted_sqls]


def minimal_set_selection(sqls: list[dict], gen_points: list[dict]):
    points_names = []
    for p in gen_points:
        points_names.append(p['point'].point_name)

    all_schemas = {}
    for db_id in get_db_ids():
        schema_path = os.path.join(get_data_path(), db_id, 'schema.json')
        if os.path.exists(schema_path):
            with open(schema_path, 'r') as f:
                all_schemas[db_id] = json.load(f)
    used_sqls = []
    for sql in sqls:
        points = sql['fulfilling_points']
        new_points = []
        for p in points:
            if p in points_names:
                new_points.append(p)
        sql['fulfilling_points'] = new_points
        if len(new_points) > 0:
            used_sqls.append(sql)
    added_points = set()
    tables = []
    # greedy algorithm to select minimal set of sqls to cover all gen_points
    selected_sqls = []
    while len(added_points) < len(gen_points):
        used_sqls = relevant_sql_sort(tables, used_sqls, added_points, all_schemas)
        best_sql = used_sqls.pop(0)
        cnt = 0
        for p in best_sql['fulfilling_points']:
            if p not in added_points:
                cnt += 1
        if cnt == 0:
            print('Error to find best sql')
            break
        selected_sqls.append(best_sql)
        for p in best_sql['fulfilling_points']:
            added_points.add(p)
        for tbl in best_sql['tables']:
            if tbl not in tables:
                tables.append(tbl)
    return selected_sqls
