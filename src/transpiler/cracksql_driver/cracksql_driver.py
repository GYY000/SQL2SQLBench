import os.path

from cracksql.cracksql import translate, initkb
from readerwriterlock import rwlock
from utils.tools import get_proj_root_path, load_mysql_config, load_pg_config, load_oracle_config

mysql_config = load_mysql_config()
pg_config = load_pg_config()
ora_config = load_oracle_config()

init_flag = False
init_lock = rwlock.RWLockFairD()


def init_kb_func():
    global init_flag
    with init_lock.gen_rlock():
        if init_flag:
            return
    with init_lock.gen_wlock():
        if init_flag:
            return
        init_flag = True
    try:
        cracksql_config_path = os.path.join(get_proj_root_path(), 'src', 'transpiler', 'cracksql_driver',
                                            'init_config.yaml')
        initkb(cracksql_config_path)  # fill the basic configurations in the `.yaml` first
        print("Knowledge base initialized successfully")
    except Exception as e:
        print(f"Knowledge base initialization failed: {str(e)}")
        import traceback
        traceback.print_exc()


def trans_func(sql, src_dialect, tgt_dialect, db_name, model_name, db_para=None, out_dir: str='./'):
    # global init_flag
    # if not init_flag:
    #     init_flag = True
    if tgt_dialect == 'oracle':
        target_db_config = {
            "host": ora_config['oracle_host'],
            "port": ora_config['oracle_port'],
            "user": db_name,
            "password": ora_config['usr_default_pwd'],
            "db_name": ora_config['oracle_sid']
        }
    elif tgt_dialect == 'pg':
        target_db_config = {
            "host": pg_config['pg_host'],
            "port": pg_config['pg_port'],
            "user": pg_config['pg_user'],
            "password": pg_config['pg_pwd'],
            "db_name": db_name
        }
    else:
        target_db_config = {
            "host": mysql_config['mysql_host'],
            "port": mysql_config['mysql_port'],
            "user": mysql_config['mysql_user'],
            "password": mysql_config['mysql_pwd'],
            "db_name": db_name
        }
    dialect_mapping = {
        "mysql": "mysql",
        "pg": "postgresql",
        "oracle": "oracle",
    }
    src_dialect = dialect_mapping[src_dialect]
    tgt_dialect = dialect_mapping[tgt_dialect]

    vector_config = {
        "src_kb_name": f"{src_dialect}_knowledge",
        "tgt_kb_name": f"{tgt_dialect}_knowledge"
    }

    try:
        translated_sql, model_ans_list, used_pieces, lift_histories = translate(
            model_name=model_name,
            src_sql=sql,
            src_dialect=src_dialect,
            tgt_dialect=tgt_dialect,
            target_db_config=target_db_config,
            vector_config=vector_config,
            out_dir=out_dir,
            retrieval_on=True,
            top_k=3,
            db_para=db_para
        )
        return True, translated_sql, model_ans_list, used_pieces, lift_histories
    except Exception as e:
        return False, f"Error occurred during translation: {str(e)}", None, None, None


# init_kb_func()
