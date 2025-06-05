import boto3
import pandas as pd
from utilities.extract_dataframe_from_event import extract_dataframes_from_event

from utilities.s3.get_cache_missing_table import cache_missing_table

REQUIRED_TABLES = {
    'design',
    'counterparty',
    'address',
    'currency',
    'staff',
    'department',
    'transaction',
    'sales_order'
}

def lambda_handler(event, context):
    # TODO:
    # Add logging to handler
    
    s3_client = boto3.client('s3')
    cached_dataframes = {}

    event_dataframes = extract_dataframes_from_event(s3_client, event)
    missing_tables = REQUIRED_TABLES - set(event_dataframes.keys())
    
    for table in missing_tables:
        missing_table_dataframe = cache_missing_table(s3_client, event_dataframes, table)
        cached_dataframes[table] = missing_table_dataframe
        
    # Combine cached_dataframes with event_dataframes
    
    # Transform dataframes into dimensions -> facts