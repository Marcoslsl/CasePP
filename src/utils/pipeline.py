import pandas as pd
from typing import List


class Pipeline:
    """Pipeline class."""

    def __init__(self) -> None:
        """Constructor"""
        pass

    @staticmethod
    def validate_input_data(data: str) -> bool:
        """Validate the input data.

        Parameters
        ----------
        data: str
            path to be validated.
        """
        if isinstance(data, str):
            if data.endswith(".csv"):
                return True
            else:
                raise ValueError("data must be a csv file.")
        else:
            raise ValueError("data must be a string")

    def __read_data(
        self, data_bank: str, data_transactions: str, sep: str = ","
    ) -> pd.DataFrame:
        """Read data from csv files.

        Parameters
        ----------
        data_bank: str
            Must be the path for bank_dim csv file.
        data_transactions: str
            Must be the path for transactions csv file.
        sep: str, default = ","
            Delimiter to use.
        """
        self.validate_input_data(data_bank)
        self.validate_input_data(data_transactions)

        transactions = pd.read_csv(data_bank, sep=sep)
        bank_dim = pd.read_csv(data_bank, sep=sep)

        return transactions.merge(bank_dim, on="bank_id", how="left")

    def __transform_data(
        self, transactions_bank: pd.DataFrame
    ) -> pd.DataFrame:
        """Transform data.

        Parameters
        ----------
        transactions_bank: pd.Dataframe
            Merged dataframe between bank_dim and transactions.
        """
        transactions_bank["transaction_name_treated"] = transactions_bank[
            "transaction_name_treated"
        ].apply(lambda x: x.upper())

        transactions_bank["transaction_amount"] = (
            transactions_bank["transaction_amount"]
            .apply(lambda x: x.replace(",", "."))
            .astype("float64")
        )

        types = {
            "transaction_id": str,
            "user_id": str,
            "year": str,
            "month": str,
            "day": str,
            "bank_id": str,
        }
        transactions_bank = transactions_bank.astype(types)

        transactions_bank["year_month"] = (
            transactions_bank["year"].astype("str")
            + "-"
            + transactions_bank["month"].astype("str")
        )

        return transactions_bank.pivot_table(
            index=["user_id", "year_month", "bank_name"],
            columns="transaction_name_treated",
            values="transaction_amount",
            aggfunc="sum",
        ).reset_index()

    def __load_data(
        self, data: pd.DataFrame, path: str = None
    ) -> pd.DataFrame:
        """Load data.

        Parameters
        ----------
        data: pd.Dataframe
        path: str, default = None
            The path to save the dataframe.
        """
        if path is not None:
            data.to_csv("RESULT.csv")
        return data

    def run_pipeline(
        self,
        data_bank: str,
        data_transactions: str,
        sep: str = ",",
        path: str = None,
    ) -> pd.DataFrame:
        """Run pipeline.

        Parameters
        ----------
        data_bank: str
            Must be the path for bank_dim csv file.
        data_transactions: str
            Must be the path for transactions csv file.
        sep: str, default = ","
            Delimiter to use.
        path: str: default = None
            The path to save the dataframe.
        """
        transactions_bank = self.__read_data(data_bank, data_transactions, sep)
        transactions_bank = self.__transform_data(transactions_bank)
        return self.__load_data(data=transactions_bank, path=path)
