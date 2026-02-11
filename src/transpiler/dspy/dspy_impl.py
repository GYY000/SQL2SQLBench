import json
import random

import dspy
import sqlglot
from typing import Any, Iterable, Tuple

from dspy import Evaluate
from tqdm import tqdm

from transpiler.tool import fetch_db_param, create_stmt_fetch
from utils.ExecutionEnv import ExecutionEnv
from utils.db_connector import sql_execute
from utils.tools import get_all_db_name, get_proj_root_path
from verification.verify import tol_order_unaware_compare

SQLGLOT_DIALECT_MAP = {
    "postgresql": "postgres",
    "postgre": "postgres",
    "postgres": "postgres",
    "mysql": "mysql",
    "oracle": "oracle",
    "sqlserver": "tsql",
    "mssql": "tsql",
    "tsql": "tsql",
    "snowflake": "snowflake",
    "bigquery": "bigquery",
    "sqlite": "sqlite",
}


def to_sqlglot_dialect(name: str) -> str:
    key = (name or "").strip().lower()
    return SQLGLOT_DIALECT_MAP.get(key, key)


def normalize_sql(sql: str, dialect: str) -> str:
    d = to_sqlglot_dialect(dialect)
    expr = sqlglot.parse_one(sql, read=d)  # AST
    return expr.sql(dialect=d, pretty=False)


class SQLTranslationInput(dspy.Signature):
    source_dialect = dspy.InputField(desc="Source dialect, e.g. MySQL, Oracle, SQL Server")
    target_dialect = dspy.InputField(desc="Target dialect, e.g. PostgreSQL, Snowflake")
    source_sql = dspy.InputField(desc="The source SQL to translate")
    schema_info = dspy.InputField(desc="DDL/schema info for involved tables")
    db_param = dspy.InputField(desc="The parameter of the source or target database")
    type_mapping = dspy.InputField(desc="The type mapping policy between the source and target database")
    target_sql = dspy.OutputField(desc="Equivalent SQL in target dialect")



class SQLTranslator(dspy.Module):
    def __init__(self):
        super().__init__()
        self.prog = dspy.ChainOfThought(SQLTranslationInput)

    def forward(self, source_dialect, target_dialect, source_sql, schema_info, db_param, type_mapping):
        return self.prog(
            source_dialect=source_dialect,
            target_dialect=target_dialect,
            source_sql=source_sql,
            schema_info=schema_info,
            db_param=db_param,
            type_mapping=type_mapping
        )


def validate_sql_accuracy(example, pred, trace=None) -> float:
    try:
        flag1, rows_source = sql_execute(example.source_dialect, get_all_db_name(example.source_dialect), example.target_sql,
                                  example.db_param.get(example.source_dialect, {}), restart_flag=True)
        flag2, rows_target = sql_execute(example.target_dialect, get_all_db_name(example.target_dialect), pred.target_sql,
                                  example.db_param.get(example.target_dialect, {}), restart_flag=True)
        if flag2:
            if tol_order_unaware_compare(rows_source, rows_target):
                return 1.0
            else:
                return 0.2
        return 0.0
    except Exception as e:
        return 0.0

def combined_metric(example, pred, trace=None):
    return validate_sql_accuracy(example, pred, trace)


from dspy.teleprompt import BootstrapFewShot, BootstrapFewShotWithRandomSearch


def train_dspy():
    type_mapping_table = {
        "mysql": {
            "pg": {
                "YEAR": "SMALLINT",
                "POINT": "GEOGRAPHY",
                "BLOB": "BYTEA"
            },
            "oracle": {
                "YEAR": "NUMBER(4)",
                "POINT": "SDO_GEOMETRY",
                "BOOL": "NUMBER(1)"
            },
        },
        "pg": {
            "mysql": {
                "UUID": "CHAR(36)",
                "GEOGRAPHY": "POINT",
                "JSONB": "JSON",
                "XML": "TEXT",
                "ARRAY": "JSON",
            },
            "oracle": {
                "UUID": "CHAR(36)",
                "GEOGRAPHY": "SDO_GEOMETRY",
                "JSONB": "JSON",
                "XML": "XMLType",
                "ARRAY": "VARRAY",
                "BOOL": "NUMBER(1)"
            }
        },
        "oracle": {
            "mysql": {
                "SDO_GEOMETRY": "POINT",
                "XMLType": "XML",
                "VARRAY": "JSON",
            },
            "pg": {
                "SDO_GEOMETRY": "GEOGRAPHY",
                "XMLType": "XML",
                "VARRAY": "ARRAY"
            },
            "snowflake": {
                "SDO_GEOMETRY": "GEOGRAPHY",
                "XMLType": "VARIANT",
                "VARRAY": "ARRAY",
            },
            "sqlserver": {
                "SDO_GEOMETRY": "GEOGRAPHY",
                "XMLType": "XML",
                "VARRAY": "NVARCHAR(MAX)",
            }
        }
    }

    with open(f'{str(get_proj_root_path())}/src/transpiler/dspy/testset.json', 'r') as file:
        sqls = json.load(file)
    all_examples = []
    for sql in tqdm(sqls):
        src_dialect = None
        tgt_dialect = None
        model_dialect_map = {
            "pg": "PostgreSQL",
            "mysql": "MySQL",
            "oracle": "Oracle",
            'sqlserver': "SQL Server",
            "snowflake": "Snowflake"
        }
        for key, value in sql.items():
            if src_dialect is None:
                src_dialect = key
            elif tgt_dialect is None:
                tgt_dialect = key
        db_param = {
            src_dialect: {},
            tgt_dialect: {}
        }
        for point1 in sql['points']:
            point_db_param = fetch_db_param(point1['point'], src_dialect, tgt_dialect)
            for key, value in point_db_param.items():
                db_param.update({key: value})
        all_examples.append(
            dspy.Example(
                source_dialect=src_dialect,
                target_dialect=tgt_dialect,
                source_sql=sql[src_dialect],
                schema_info='\n'.join(create_stmt_fetch(sql['tables'], src_dialect)),
                db_param=db_param,
                target_sql=sql[tgt_dialect],
                type_mapping=json.dumps(type_mapping_table[src_dialect][tgt_dialect], indent=2),
            ).with_inputs('source_dialect', 'target_dialect', 'source_sql', 'schema_info', 'db_param', 'type_mapping')
        )

    random.seed(42)  # 固定随机种子以便复现
    random.shuffle(all_examples)
    trainset = all_examples[:100]
    devset = all_examples[100:]

    import dspy

    lm = dspy.LM(
        "openai/deepseek-r1-250528",  # 这里的前缀 openai/ 代表“OpenAI-compatible”
        api_base="Your api base",
        api_key="Your api key",  # 内网不需要就填 ""
        model_type="chat",  # 通常用 chat
    )
    dspy.configure(lm=lm)

    optimizer = BootstrapFewShotWithRandomSearch(
        metric=combined_metric,
        max_bootstrapped_demos=3,
        max_labeled_demos=3,
        num_candidate_programs=6,
        num_threads=4
    )

    dspy.settings.configure(show_progress=True)

    compiled_sql_translator = optimizer.compile(SQLTranslator(), trainset=trainset)

    file_path = f"{str(get_proj_root_path())}/src/transpiler/dspy/compiled_sql_translator.json"
    compiled_sql_translator.save(file_path)

    print(f"Model have been saved to {file_path}")


def generate_report(program, train_data, dev_data, name="Model"):
    eval_train = Evaluate(devset=train_data, num_threads=4)
    eval_dev = Evaluate(devset=dev_data, num_threads=4)

    train_acc = eval_train(program, metric=combined_metric)
    dev_acc = eval_dev(program, metric=combined_metric)

    print(f"\n--- {name} Report ---")
    print(f"Train Acc: {train_acc}%")
    print(f"Dev Acc: {dev_acc}%")
    return train_acc, dev_acc

# generate_report(SQLTranslator(), trainset, devset, name="Zero-Shot")
# generate_report(compiled_sql_translator, trainset, devset, name="Few-Shot Optimized")
