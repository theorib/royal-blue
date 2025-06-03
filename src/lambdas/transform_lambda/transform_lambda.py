from src.typing_utils import EmptyDict

event = {
    "files_to_process": [
        {
            "table_name": "currency",
            "key": "2025/06/13/currency_2025-06-13_10-35-20_012023.parquet",
            # "last_updated": new_table_data_last_updated,
            # "file_name": filename,
            # "is_transformed": False,
            # changed timestamp to last_updated
            # "is_loaded": False,
        },
    ]
}

temp = {
    "Records": [
        {
            "eventVersion": "2.1",
            "eventSource": "aws:s3",
            "awsRegion": "eu-west-1",
            "eventTime": "2025-06-02T12:00:00.000Z",
            "eventName": "ObjectCreated:Put",
            "s3": {
                "bucket": {"name": "ingestion-zone-bucket"},
                "object": {
                    "key": "2025/06/13/currency_2025-06-13_10-35-20_012023.parquet",
                    "size": 45234,
                    "eTag": "abc123etag",
                    "sequencer": "005C6F8F4E82D3AAE6",
                },
            },
        }
    ]
}

detail = {
    "table_name": "currency",
    "key": "2025/06/13/currency_2025-06-13_10-35-20_012023.parquet",
    "last_updated": "2025-06-02T12:00:00.000Z",
    "file_name": "currency_2025-06-13_10-35-20_012023.parquet",
    "is_transformed": False,
}

# assuming that we can process each parquet individually and that a parquet/dataframe does not depend on others in order to be transformed


def lambda_handler(event: EmptyDict, context, EmptyDict):
    """
    1. create an s3 client
    s3_client = boto3.....

    2. Get Env vars
    INGEST_ZONE_BUCKET_NAME = os.environ.get("INGEST_ZONE_BUCKET_NAME")
    LAMBDA_STATE_BUCKET_NAME = os.environ.get("LAMBDA_STATE_BUCKET_NAME")
    PROCESSED_ZONE_BUCKET_NAME = os.environ.get("PROCESSED_ZONE_BUCKET_NAME")

    3. Get parquet file details
    key = event['Records'][0]['s3']['object']['key']
    file_details = get_details_from_state(key)
    table_name = file_details['table_name']

    4. get parquet file
    parquet_file = get_file_from_s3_bucket(s3_client,INGEST_ZONE_BUCKET_NAME,key)

    5. get DataFrame from parque
    data_frame = create_data_frame_from_parquet(parquet_file)

    6. Transform data_frame into a data_frame that follows the star schema
    transformed_df = transform_dataframe_for_table(table_name, data_frame)

    7. Save Transformed data as parquet to processed s3
    transformed_parquet = create_parquets_from_data_frames(transformed_df)

    new_key = ....

    8. upload parquet to s3
    response = add_to_s3_bucket(s3_client, PROCESSED_ZONE_BUCKET_NAME, new_key, transformed_parque)

    if response.get('error'):
        logger.error(response['error']['message'])
        raise response['error']['raw_response']

    9. update transform_state
    new_state_log_entry = (
                    {
                        "table_name": table_name,
                        "last_updated": ....,
                        "file_name": filename,
                        "key": key,
                        "is_transformed": True,
                        # "is_loaded": False,
                    },
                )




    2. Log event ie. Transform lambda triggered with timestamp in eventbridge
    3. Download parquet files from s3 bucket
    4. Load into pandas data frame
    5. Identify relevant tables from s3 key
    8. Log success/error to cloud watch
    """
