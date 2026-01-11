# Data validation & reject handling (pandas)

## What this is

This repo shows how I approach data validation and reject handling in a Python / pandas pipeline.


## How it works

The pipeline splits validation into two categories:

### Fatal checks (fail fast)

These stop the pipeline immediately if:
- the source data is empty
- required columns are missing
- the primary key (`customer_id`) is duplicated

These indicate structural problems where it’s unsafe to continue.


### Row-level validation

Individual rows are rejected (but processing continues) if they fail rules such as:
- invalid or missing `customer_id`
- null or empty required fields
- invalid email format
- country not in the allowed list (`UK`, `US`, `CA`)

A single row of data can fail on multiple rules. Rule breaches are reported in a rejection file and are grouped by customer id (i.e. one row per customer id)


## Outputs

The pipeline produces two DataFrames:

### Valid data
Only rows that pass all validation rules.  
Schema is enforced and values are cleaned.

### Rejected data
All rejected rows, including:
- original source columns
- a pipe-delimited `rejection_reasons` field
- a `rejection_timestamp`

This makes it easier to see exactly why data was rejected


## Sample data

The input data is intentionally hard-coded sample data. his keeps the example easy to run and deterministic. In a real pipeline, the source DataFrame would come from a file, database, or API.


