from src.typing_utils import EmptyDict


def lambda_handler(event: EmptyDict, context, EmptyDict):
    """
    1. Get current timestamp
    2. Log event ie. Transform lambda triggered with timestamp in eventbridge
    3. Download parquet files from s3 bucket
    4. Load into pandas data frame
    5. Identify relevant tables from s3 key
    6. Transform data to star schema
    7. Save Transformed data as parquet to processed s3
    8. Log success/error to cloud watch
    """