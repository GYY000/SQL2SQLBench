from utils.db_connector import sql_execute, oracle_sql_execute, mysql_drop_db, oracle_drop_db, pg_drop_db
from utils.tools import get_table_col_name, get_all_db_name, get_db_ids, get_empty_db_name

from tqdm import tqdm
import json


def drop_schema(dialect: str):
    """
    Drop a schema from the database.
    """
    db_id = 'bird'
    with open(f'/home/gyy/data/database_data/{db_id}/schema.json', 'r') as f:
        data = json.load(f)
    db_name = get_all_db_name(dialect)
    for key, value in tqdm(data.items()):
        flag, res = sql_execute(dialect, db_name, f'DROP TABLE {get_table_col_name(key, dialect)};')
        if flag:
            print(f"DROP TABLE {get_table_col_name(key, dialect)}")
        flag, res = sql_execute(dialect, get_empty_db_name(db_name), f'DROP TABLE {get_table_col_name(key, dialect)};')
        if flag:
            print(f"DROP TABLE {get_table_col_name(key, dialect)}")
