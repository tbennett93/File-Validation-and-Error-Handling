import pandas as pd
from typing import Collection
from datetime import datetime
import pathlib

#Contract
schema = {
    "customer_id":{
        "type": 'int64',
        "nullable":False
    },
    "name":{
        "type": 'string',
        "nullable":False
    },
    "email":{
        "type": 'string',
        "nullable":False
    },
    "country":{
        "type": 'string',
        "nullable":False
    }
}


df_sample_data = pd.DataFrame({
    "customer_id": [1, 2, None, 4, 5, 6, 7], #None=invalid
    "name": [
        "Alice", 
        "Bob", 
        "David", 
        None,                   # invalid
        "Eve", 
        "",                     # invalid
        "Mary"
    ],
    "email": [
        "alice@example.com",
        "bobexample.com",      # invalid
        "david@example.com",
        None,                  # invalid
        "eve@example.com",
        "john_email",          # invalid
        ""                     # invalid
    ],
    "country": [
        "UK", 
        "FR",                  # invalid
        "UK", 
        "UK", 
        "US ",                 # valid with a trim
        " CA",                 # valid with a trim
        "USA"                  # invalid
    ],  
})

df : pd.DataFrame = df_sample_data

def file_rejection(df: pd.DataFrame):
        
    def empty_df(df: pd.DataFrame):
        if df.size == 0:
            raise ValueError("Data source is empty")

    def required_column_missing(df: pd.DataFrame):
        df = {x for x in df.columns}

        expected_columns = {x for x in schema.keys()}

        missing_columns = expected_columns - df

        if len(missing_columns) > 0:
            raise ValueError(f"Columns missing from source: {missing_columns}")
    
    def duplicate_pk(df: pd.DataFrame):
        duplicate_ids :pd.Series = df.groupby("customer_id").size() 
        duplicate_ids = duplicate_ids[duplicate_ids > 1].index.tolist()
        if len(duplicate_ids) > 0:
            raise ValueError(f"Duplicate customer IDs: {duplicate_ids}")


    empty_df(df)
    required_column_missing(df)
    duplicate_pk(df)


def row_level_validation(df: pd.DataFrame, df_reject: pd.DataFrame):

    def format_customer_id(df: pd.DataFrame ) -> pd.DataFrame :
        df["customer_id"] = pd.to_numeric(df["customer_id"], errors='coerce')
        return df

    def customer_id_validation(df: pd.DataFrame, df_reject: pd.DataFrame):
        errors : pd.DataFrame = df[df["customer_id"].isna()].copy()
        errors["rejection_reason"] = "Invalid customer_id"
        df_reject = pd.concat([errors, df_reject]).drop_duplicates()
        return df_reject
    
    def customer_id_schema_enforce(df: pd.DataFrame):
        df = df[~df["customer_id"].isna()].copy()
        df["customer_id"] = df["customer_id"].astype('int64')
        return df
        
    def enforce_schema_strings(df: pd.DataFrame, string_cols: Collection[str]) -> pd.DataFrame:
        for col in string_cols:
            df[col] = df[col].str.strip().astype('string')
        return df

    def validation_country(df: pd.DataFrame, countries: Collection[str], df_reject: pd.DataFrame):
        errors = df[~df["country"].isin(countries)].copy()
        errors["rejection_reason"] = "Invalid country"
        df_reject = pd.concat([errors, df_reject]).drop_duplicates()
        return df_reject
        
    def enforce_validation_country(df: pd.DataFrame, countries: Collection[str]):
        df = df[df["country"].isin(countries)].copy()
        return df
    
    def validation_null_required_fields(df: pd.DataFrame, req_fields: Collection[str], df_reject: pd.DataFrame) -> pd.DataFrame:
        for col in req_fields:
            errors = df[(df[col].isna()) | (df[col]=="")].copy()
            errors["rejection_reason"] = f"Null required field: {col}"
            df_reject = pd.concat([errors, df_reject]).drop_duplicates()            
        return df_reject

    def enforce_validation_null_required_fields(df: pd.DataFrame, required_fields: Collection[str]):
        for field in required_fields:
            df = df[(~df[field].isna()) & (df[field] != "")].copy()
        return df

    def validation_email(df: pd.DataFrame, df_reject: pd.DataFrame):
        errors = df[(df["email"] != "") & (~df["email"].isna())].copy() #stops blank email being validated twice
        errors = errors[~errors["email"].str.contains(EMAIL_REGEX, regex=True)].copy()
        errors["rejection_reason"] = 'invalid email'
        df_reject = pd.concat([errors, df_reject]).drop_duplicates()      
        return df_reject

    def enforce_validation_email(df: pd.DataFrame):
        return df[df["email"].str.contains(EMAIL_REGEX, regex=True)].copy()
    

    #validate and format strings
    string_cols = {col for col, reqs in schema.items() if reqs["type"] == 'string'}
    df = enforce_schema_strings(df, string_cols)

    #validate and format customer id
    df = format_customer_id(df)
    df_reject = customer_id_validation(df, df_reject)
    df = customer_id_schema_enforce(df)

    #validate and format null required fields
    required_fields= {col for col, reqs in schema.items() if not reqs["nullable"]}
    df_reject = validation_null_required_fields(df, required_fields, df_reject)

    #validate and format countries
    df["country"] = df["country"].str.upper()
    allowed_countries = {"UK","US","CA"}
    df_reject = validation_country(df, allowed_countries, df_reject)

    #validate and format email
    df_reject = validation_email(df, df_reject)
    
    #reject errors from output
    df = enforce_validation_null_required_fields(df, required_fields)
    df = enforce_validation_country(df, allowed_countries)
    df = enforce_validation_email(df)

    #produce rejection df - one row per customer id, rejection reasons grouped and pipe-delimited
    df_reject = (df_reject
                .groupby(["customer_id","name","email","country"], as_index=False, dropna=False)
                .agg(
                    rejection_reasons = ("rejection_reason", lambda x: "|".join(x.astype('string')))
                )
                
    )

    df_reject["rejection_timestamp"] = datetime.now()
    df_reject["rejection_reasons"] = df_reject["rejection_reasons"].astype('string')

    return df, df_reject


def output_file(df: pd.DataFrame, filepath: pathlib.Path):
    df.to_csv(filepath.absolute(), index=False)


#main
EMAIL_REGEX = r'^(?!.*\.\.)[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$'

file_rejection(df)

df, df_reject = row_level_validation(df, df.head(0)) #df.head() passes df schema to populate

if df.empty and not df_reject.empty:
    raise ValueError("Fatal error: all rows rejected.")


#output main and rejection dfs
output_folder = r"C:\..."
output_folder = pathlib.Path(output_folder)
main_csv_output_path = output_folder / f'error_handing_and_failure_strategy_main_{datetime.now().strftime("%F %H%M%S")}.csv'
reject_csv_output_path= output_folder / f'error_handing_and_failure_strategy_reject_{datetime.now().strftime("%F %H%M%S")}.csv'
output_file(df, main_csv_output_path)
output_file(df_reject, reject_csv_output_path)


# print(df)
# print(df.info())

# print(df_reject)
# print(df_reject.info())

