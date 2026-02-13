# GEN_PROMPT = """
# ### Task
# Synthesize **insert-ready mock data** for the given database schema so that executing the provided SQL query on a **{src_dialect}** database produces the **exact target output** described below.
#
# ---
#
# ### 1. Database Context
# **SQL Dialect:** {src_dialect}
# **Database Parameters:** {db_params}
#
# #### Table Schemas (DDL)
# {ddls}
#
# > All generated data MUST strictly conform to the DDL definitions, including:
# > - Column data types
# > - Primary keys
# > - Foreign key relationships
# > - NULL / NOT NULL constraints
#
# ---
#
# ### 2. SQL Query
# ```sql
# {sql}
# ````
#
# ---
#
# ### 3. Objective
#
# **Target Output Requirement:**
# {target_output_desc}{error_context}
#
# Your task is to generate source table data such that:
#
# * Running the SQL query on the generated data
# * Produces **exactly** the target output (no extra rows, no missing rows)
#
# ---
#
# ### 4. Hard Constraints (MUST FOLLOW)
#
# 1. **DDL Compliance**
#
#    * Data types must match exactly
#    * Primary keys must be unique
#    * Foreign keys must reference valid rows
#
# 2. **SQL Logic Consistency**
#
#    * Reverse-engineer JOIN conditions, WHERE filters, GROUP BY, HAVING, and aggregations
#    * Ensure all predicates are satisfied by the generated data
#
# 3. **Cross-table Consistency**
#
#    * All relationships across tables must be logically and referentially consistent
#
# 4. **Minimal Data Principle**
#
#    * Generate the smallest possible dataset that still fulfills the objective
#
# ---
#
# ### 5. Data Formatting Rules (STRICT)
#
# * **Date / Timestamp**
#
#   * Use `'YYYY-MM-DD'` or `'YYYY-MM-DD HH:MM:SS'`
#
# * **POINT Type**
#
#   * Represent as a JSON object:
#
#     ```json
#     {
#         "longitude": "-86.13631",
#         "latitude": "40.485424"
#     }
#     ```
#
# * **JSON / ARRAY Columns**
#
#   * Use native JSON objects or arrays
#   * ❌ Do NOT wrap them as escaped strings
#
#   ✅ Correct:
#
#   ```json
#   "metadata": {"key": "val"}
#   ```
#
#   ❌ Incorrect:
#
#   ```json
#   "metadata": "{{\"key\": \"val\"}}"
#   ```
#
#     ---
#
#     ### 6. Reasoning Strategy (Follow Internally)
#
#     1. Analyze the SQL query to identify:
#
#    * Required columns
#    * Filters and constants
#    * Join paths and cardinality
# 2. Work backward from the target output to infer:
#
#    * Which rows must exist
#    * Which values are necessary
# 3. Synthesize a complete but minimal set of rows for every referenced table
#
# ---
#
# ### 7. Output Format (STRICT)
#
# * Output **JSON only**
# * Top-level keys are table names
# * Values are lists of row objects
#
# Example:
#
# ```json
# {{
#     "table_name": [
#     {{"col1": "val1", "col2": 10 }},
#     {{"col1": "val2", "col2": 20 }}
#   ]
# }}
# ```
# """

GEN_PROMPT = """
### Task
Synthesize or modify **insert-ready mock data** for the given database schema. 

**CRITICAL CONSTRAINT:** You are provided with a set of `Existing Data`. You must attempt to achieve the `Target Output` by minimally modifying the `Existing Data` or adding rows to it. If the `Target Output` is logically impossible to achieve given the constraints and the provided query, return exactly `null` (not a JSON object).

---

### 1. Database Context
**SQL Dialect:** {src_dialect}  
**Database Parameters:** {db_params}

#### Table Schemas (DDL)
{ddls}

---

### 2. SQL Query
```sql
{sql}

```

---

### 3. Existing Data (Base Dataset)

The following data already exists in the tables. You should use this as your starting point:
{existing_data}

---

### 4. Objective

**Target Output Requirement:**
{target_output_desc}{error_context}

Your task is to generate a complete dataset (Existing Data + Modifications) such that running the SQL query produces **exactly** the target output.

---

### 5. Hard Constraints (MUST FOLLOW)

1. **Logical Feasibility:** If the SQL logic (e.g., a specific WHERE clause or JOIN) contradicts the Target Output requirements such that no data manipulation can satisfy it, return `null`.
2. **DDL Compliance:** Data types, PKs, and FKs must be strictly valid.
3. **Minimal Modification:** Prefer modifying existing rows or adding the minimum necessary new rows over recreating the dataset from scratch.
4. **Consistency:** All relationships across tables must be referentially consistent.

---

### 6. Data Formatting Rules (STRICT)

* **Date / Timestamp:** Use `'YYYY-MM-DD'` or `'YYYY-MM-DD HH:MM:SS'`.
* **POINT Type:** Represent as a JSON object: `{{"longitude": "X", "latitude": "Y"}}`.
* **JSON / ARRAY Columns:** Use native JSON objects/arrays (do NOT wrap in escaped strings).

---

### 7. Output Format (STRICT)

* If a solution exists: Output **JSON only**. Top-level keys are table names, values are lists of row objects.
* Values are lists of row objects representing ONLY the incremental data (newly added rows) required to achieve the Target Output.
* If no solution is possible: Output **null**.

Example (Success):

```json
{{
    "table_name": [
        {{"col1": "val1", "col2": 10 }},
        {{"col1": "val2", "col2": 20 }}
    ]
}}

```

Example (Failure):
null

```
"""
