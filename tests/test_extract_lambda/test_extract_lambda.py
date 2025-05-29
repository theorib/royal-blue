import pytest


@pytest.mark.describe(
    "Check extract_lambda interaction with Totesys DB, parquet files and ingestion_zone s3 bucket"
)
class TestExtractLambdaAddParquetToIngestionZone:
    @pytest.mark.skip
    @pytest.mark.it(
        "check that it creates one file per table from totesys DB on first connection to DB saves them as parquet files in ingestion_zone s3 bucket"
    )
    def test_extracts_all_first_connection(self):
        pass

    @pytest.mark.skip
    @pytest.mark.it(
        "check that it extracts only data that was not extracted before from totesys DB and saves only the new data to ingestion_zone s3 bucket as parquet files"
    )
    def test_extracts_new_data_only(self):
        pass

    @pytest.mark.skip
    @pytest.mark.it(
        "check that saved parquet files have the expected data in the expected data structure"
    )
    def test_parquet_data(self):
        pass


@pytest.mark.describe("Check extract_lambda return values")
class TestExtractLambdaReturn:
    @pytest.mark.skip
    @pytest.mark.it(
        "check that it returns an empty list if there is no new data in totesys DB"
    )
    def test_returns_empty_list(self):
        pass

    @pytest.mark.skip
    @pytest.mark.it(
        "check that it returns a list all files (one file per DB table) which were saved to ingestion_zone s3 bucket from the first totesys DB data extraction"
    )
    def test_returns_dict_of_files_per_table(self):
        pass

    @pytest.mark.skip
    @pytest.mark.it(
        "check that it returns a list of only new files saved to ingestion_zone s3 bucket when extracting newly added data from totesys DB"
    )
    def test_returns_dict_of_new_updates(self):
        pass


@pytest.mark.describe("Check extract_lambda interactions with state_management bucket")
class TestExtractLambdaState:
    @pytest.mark.skip
    @pytest.mark.it(
        "check that after data is saved to ingestion_zone s3 bucket, that extract_lambda updates the state file in the state_management bucket"
    )
    def test_updates_state_file(self):
        pass

    @pytest.mark.skip
    @pytest.mark.it(
        "check that it updates state without adding new files if there was no new data in totesys DB"
    )
    def test_updates_state_no_new_data(self):
        pass


@pytest.mark.describe("Check extract_lambda logging")
class TestExtractLambdaLogging:
    @pytest.mark.skip
    @pytest.mark.it(
        "check that it logs a list of newly extracted data files to the console"
    )
    def test_logs_new_data(self):
        pass

    @pytest.mark.skip
    @pytest.mark.it(
        "check that it logs that no data was extracted when there is no new data to extract"
    )
    def test_logs_no_data(self):
        pass

    @pytest.mark.skip
    @pytest.mark.it("check that it logs totesys DB connection errors")
    def test_logs_db_errors(self):
        pass

    @pytest.mark.skip
    @pytest.mark.it("check that it logs boto3 s3 client errors when writing data")
    def test_logs_s3_errors(self):
        pass
