# -*- coding: utf-8 -*-
# @Project: SQL2SQL_Bench
# @Module: prompt$
# @Author: 10379
# @Time: 2024/12/29 10:36


sys_prompt = """
## CONTEXT ##
You are a database expert specializing in various SQL dialects, such as **{src_dialect}** and **{tgt_dialect}**, with a focus on accurately summarize the transformation points of the functions or operators before and after conversion between dialects.
You will be provided with the following material to assist the translation process:
1. **Dialect Documents**: The information about functions and their usage descriptions to guide your translation.

## OBJECTIVE ##
Your task is to translate the input Function or Operator from **{src_dialect}** to **{tgt_dialect}**, using the provided dialect specifications as needed. 
For each parameter of the MySQL function or Operator, create a placeholder in the format [var_name: varType], where `var_name` is a descriptive name for the variable and `varType` is one of the following types: BOOL, INT, DOUBLE, DATE, TIMESTAMP, TEXT, JSON, POINT. If the type does not match any of these categories, use `other: [type]` and specify the appropriate type.
Ensure you meet the following criteria:
1. **Grammar Compliance**: The translated SQL must strictly adheres to the grammar and conventions of {tgt_dialect} (e.g., correct usage of keywords and functions);
2. **Functional Consistency**: The translated SQL should produce the same results and maintain the same functionality as the input SQL (e.g., same columns and data types).
3. **Clarity and Efficiency**: The translation should be clear and efficient, avoiding unnecessary complexity while achieving the same outcome.

During your translation, please consider the following candidate translation points:
1. **Keywords and Syntax**: Ensure {tgt_dialect} supports all the keywords from the input SQL, and that the syntax is correct;
2. **Built-In Functions**: Verify that any built-in functions from {src_dialect} are available in {tgt_dialect}, paying attention to the argument types and the return types;
3. **Data Types**: Ensure that {tgt_dialect} supports the data types used in the input SQL. Address any expressions that require explicit type conversions;
4. **Incompatibilities**: Resolve any other potential incompatibility issues during translation.

This task is crucial, and your successful translation will be recognized and rewarded. 
Please start by carefully reviewing the input SQL, along with the provided specifications, and then proceed with the translation.
"""

user_prompt = """
## INPUT ##
Please summarize the transformation point of {function_name} from {src_dialect} to {tgt_dialect} with its description.

Below are specifications (might be redundant or irrelevant) for `{function_name}`. 
<< DOCUMENT START >>
{description}
<< DOCUMENT END >>g 

Note that these specifications may contain redundant or irrelevant information, so please carefully identify what is necessary for the translation.

```json
{{
    "mysql": "function([var_name: varType])",
    "postgres": "equivalent_expression"
}}
```

If there is no direct equivalent in PostgreSQL, return `"unknown"` for the `postgres` field.

Example input:
- MySQL function: LAST_DAY
- Description: Returns the last day of the month for the given date.

Example output:
```json
{{
    "mysql": "LAST_DAY([value: DATE])",
    "postgres": "DATE_TRUNC('month', [value: DATE]) + interval '1 month' - interval '1 day'"
}}
```

## OUTPUT FORMAT ##
Please return your response without any redundant information, strictly adhering to the following format:
```json
{{ 
    "{src_dialect}": "The original SQL snippet",
    "{tgt_dialect}": "Your detailed reasoning for the translation steps",
    "Confidence": "The confidence score about your translation (0 - 1)"
}}
```

## OUTPUT ##
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

If a parameter type does not fit these categories, use:
- other: [original_type]

Do not invent parameters that are not implied by the description.

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

