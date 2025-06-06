import pandas as pd


def dim_currency_dataframe(dataframes: dict):
    try:
        currency_lookup = {
            "USD": "US Dollar",
            "EUR": "Euro",
            "GBP": "British Pound",
            "JPY": "Japanese Yen",
            "AUD": "Australian Dollar",
            "CAD": "Canadian Dollar",
            "CHF": "Swiss Franc",
            "CNY": "Chinese Yuan",
            "NZD": "New Zealand Dollar",
            "SEK": "Swedish Krona",
            "NOK": "Norwegian Krone",
            "MXN": "Mexican Peso",
            "INR": "Indian Rupee",
            "BRL": "Brazilian Real",
            "ZAR": "South African Rand",
            "SGD": "Singapore Dollar",
            "HKD": "Hong Kong Dollar",
            "KRW": "South Korean Won",
            "RUB": "Russian Ruble",
            "TRY": "Turkish Lira",
        }
        currency_df = dataframes.get("currency")
        if currency_df is None:
            raise ValueError("Error: 'currency' DataFrame is missing.")

        lookup_df = (
            pd.DataFrame.from_dict(
                currency_lookup, orient="index", columns=["currency_name"]
            )
            .reset_index()
            .rename(columns={"index": "currency_code"})
        )

        currency_df = currency_df.merge(
            lookup_df[["currency_code", "currency_name"]],
            how="left",
            on="currency_code",
        )

        dim_currency = currency_df[["currency_id", "currency_code", "currency_name"]]
        return dim_currency

    except Exception as e:
        raise e
