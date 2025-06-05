import pandas as pd
import pytest

from src.lambdas.transform_lambda.dimensions.dim_design_transform import (
    dim_design_dataframe,
)


@pytest.fixture
def mock_design_df():
    return pd.DataFrame(
        {
            "design_id": [1, 25],
            "created_at": ["02/04/2021", "30/05/2024"],
            "design_name": ["Logo", "Motif"],
            "file_location": ["/designs/logos/", "/designs/motifs/"],
            "file_name": ["bratz_logo.png", "barbie_motif.png"],
        }
    )


def test_dim_design_in_DF(mock_design_df):
    df = {"design":mock_design_df}
    result = dim_design_dataframe(df)

    assert not result.empty
    assert list(result.columns) == ["design_id", "design_name", "file_location", "file_name"]
    assert len(result) == 2


def test_dim_design_missing_design_table(mock_design_df):
    pass


def test_dim_design_empty_DF(mock_design_df):
    pass


def test_dim_design_duplicates(mock_design_df):
    pass


def test_dim_design_missing_columns(mock_design_df):
    pass


def test_dim_design_unrelated_columns(mock_design_df):
    pass
