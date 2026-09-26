import pandas as pd
import pytest

from src.data_quality import validate_primary_key


def test_valid_primary_key():
    df = pd.DataFrame({
        "ACCIDENT_NO": ["A001", "A002", "A003"]
    })

    assert validate_primary_key(df, "ACCIDENT_NO") is True


def test_duplicate_primary_key():
    df = pd.DataFrame({
        "ACCIDENT_NO": ["A001", "A001", "A003"]
    })

    with pytest.raises(ValueError, match="duplicate"):
        validate_primary_key(df, "ACCIDENT_NO")


def test_null_primary_key():
    df = pd.DataFrame({
        "ACCIDENT_NO": ["A001", None, "A003"]
    })

    with pytest.raises(ValueError, match="null"):
        validate_primary_key(df, "ACCIDENT_NO")