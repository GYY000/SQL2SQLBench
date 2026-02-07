# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: prompt$
# @Author: 10379
# @Time: 2024/12/29 10:36


sys_prompt = """
## CONTEXT ##
You are an expert in SQL dialect conversion and rule inference.

Your task is to infer reusable SQL transformation rules that convert SQL
from {src_dialect} to {tgt_dialect} based on a failed or incorrect conversion case.

Focus only on:
- SQL syntax, functions, operators, and semantics
- Declarative and deterministic behavior

You must preserve semantics and NULL behavior, and avoid one-off fixes.

## CRITICAL CONSTRAINTS ##
1) Output ONLY rules that cause an actual change from {src_dialect} to {tgt_dialect}.
   - Do NOT output identity / no-op rules where the source and target patterns are the same
     (ignoring whitespace/casing).
2) Do NOT include rules for SQL segments that are already valid and equivalent in both dialects.
3) Do NOT output the converted SQL itself. Only output reusable rules.

Output must strictly follow the user-specified format.
"""

user_prompt = """
I will provide information about a problematic SQL conversion.

## INPUT ##
- Source Dialect: {src_dialect}
- Target Dialect: {tgt_dialect}
- Database Parameters: {db_param}
- SQL to be translated:
{sql}
- Error Info: {error_info}
- Retrieved Similar Info: {retrieved_info}

## TASK ##
Infer reusable rules that can be used to convert this SQL
from {src_dialect} to {tgt_dialect}.
Do NOT output the converted SQL itself.
You should first determine which part of the SQL should be translated and then generate the rules to translate these parts one by one.

## Rule Definition ##

Each rule must be a JSON object with the following structure:

{{
  "Desc": "string describing the rule",
  "{src_dialect}": "source pattern used to match the SQL segment",
  "{tgt_dialect}": "target pattern that the source pattern translates to",
  "Tag": {{
    "DB PARAMETER": {{
      "{src_dialect}": {{
        "param_name": "param_value",
        ...
      }},
      "{tgt_dialect}": {{
        "param_name": "param_value",
        ...
      }}
    }}
  }}
}}

The "Tag" field is OPTIONAL and should be included only when required.

Represent the source and target pattern using the following syntax:

(<Keyword> | <Variable>) *

Keyword can any usable keyword used in databases like +, -, SELECT

Variables can be any value or expressions of a given type.
- First occurrence of a variable:
  <variable_name: variable_type>
- Subsequent occurrences:
  <variable_name>

## CRITICAL CONSTRAINTS ##
1) Output ONLY rules that cause an actual change from {src_dialect} to {tgt_dialect}.
   - Do NOT output identity / no-op rules where the source and target patterns are the same
     (ignoring whitespace/casing).
2) Do NOT include rules for SQL segments that are already valid and equivalent in both dialects.
3) Do NOT output the converted SQL itself. Only output reusable rules.

## VARIABLE TYPE POLICY (VERY IMPORTANT) ##
- You MUST use ONLY the allowed variable_type values:
  BOOL, INT, DOUBLE, DATE, TIMESTAMP, STRING, JSON, POINT, ANY_VALUE, INTERVAL

## OUTPUT (Strict) ##
Return ONLY a JSON array of rule objects.

```json
[
    {{
      "Desc": "string describing the rule",
      "{src_dialect}": "source pattern used to match the SQL segment",
      "{tgt_dialect}": "target pattern that the source pattern to be translates to",
      "Tag": {{
        "DB PARAMETER": {{
          "{src_dialect}": {{
            "param_name": "param_value",
            ...
          }},
          "{tgt_dialect}": {{
            "param_name": "param_value",
            ...
          }}
        }}
      }}
    }}
]
```

**Example Instance:**
[
    {{
      "Desc": "Convert Oracle ADD_MONTHS to MySQL TIMESTAMPADD",
      "oracle": "ADD_MONTHS ( <date_value: DATE> , <months: INT> )",
      "mysql": "TIMESTAMPADD ( MONTH , <months> , <date_value> )"
    }}
]

If no rule can be inferred, return [].
"""


sys_prompt_bat = """
You are an expert in SQL dialect translation, specializing in translating {src_dialect} functions and operators into {tgt_dialect} equivalents.

Your task is strictly limited to:
- SQL functions and operators
- Deterministic, declarative SQL expressions

You may be provided with a reference list of {tgt_dialect} functions and their descriptions.
This reference is OPTIONAL and is meant to help you identify native equivalents.
You must NOT assume the reference list is complete.

You must:
- Preserve semantic equivalence
- Prefer native {tgt_dialect} functions when available
- Use composed expressions only when no single native function exists
- Never introduce logic that changes NULL behavior unless explicitly stated
- Never rename variables or introduce new variables

All outputs must follow the exact format specified by the user.
"""

user_prompt_bat = """
I will provide you with a **{src_dialect} function or operator** and its **description**.

Optionally, I may also provide a list of **{tgt_dialect} functions and their descriptions** for reference.
These are provided only as hints and do not change the task requirements.

Your task is to convert it into its {tgt_dialect} equivalent by following the steps below.

---

### Step 1: Input
- **{src_dialect} Function or Operator**: `{function_name}`
- **Description**: `{description}`

Optional:
- **{tgt_dialect} Reference Functions**:
{ref_func_descriptions}

---

### Step 2: Formalize {src_dialect} Function

Represent the function or operator using the following syntax:

Rules for variable annotation:
- When a variable is introduced for the FIRST time, annotate it as:
  <variable_name: variable_type>
- If the SAME variable appears again, represent it ONLY as:
  <variable_name>
- Do NOT repeat the variable type after its first introduction.

Where `variable_type` must be one of:
- BOOL
- INT
- DOUBLE
- DATE
- TIMESTAMP
- TEXT
- JSON
- POINT
- ANY_VALUE
- INTERVAL

---

### Step 3: Find {tgt_dialect} Equivalent

- Use the same variable names and types as in Step 2
- Prefer a single native {tgt_dialect} function if available
- If no direct equivalent exists, use a composed SQL expression
- If no reasonable equivalent exists, return `"unknown"`

---

### Step 4: Output Format (Strict)

Return **only** the following JSON object:
- No additional keys
- No comments
- No explanatory text
- No markdown outside the JSON

```json
{{
  "{src_dialect}": "...",
  "{tgt_dialect}": "..."
}}
```
"""

