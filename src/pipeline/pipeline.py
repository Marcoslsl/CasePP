import pandas as pd
import logging
from logging import FileHandler, StreamHandler, INFO


logging.basicConfig(
    level=INFO,
    format="%(levelname)s:%(asctime)s:%(message)s",
    handlers=[FileHandler("src/logs.log", "a"), StreamHandler()],
)


class Pipeline:
    """Pipeline class."""

    def __init__(self) -> None:
        """Constructor"""
        pass

    def __repr__(self) -> str:
        return "Pipeline()"

    @staticmethod
    def __validate_columns_read(
        bank_dim: pd.DataFrame, transactions: pd.DataFrame
    ) -> bool:
        """Validate the received columns.

        Parameters
        ----------
        bank_dim: pd.Dataframe
        transactions: pd.DataFrame
        """
        bank_columns_list = ["bank_name", "bank_id"]
        transactions_columns_list = [
            "transaction_id",
            "user_id",
            "transaction_name_raw",
            "transaction_name_treated",
            "transaction_amount",
            "year",
            "month",
            "day",
            "bank_id",
        ]
        column_bank_val = all(
            column in bank_dim.columns for column in bank_columns_list
        )
        column_transaction_val = all(
            column in transactions.columns
            for column in transactions_columns_list
        )
        if column_bank_val and column_transaction_val:
            return True
        else:
            msg = "Columns validation error"
            logging.error(msg)
            raise ValueError(msg)

    @staticmethod
    def __validate_input_data(data: str) -> bool:
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
                msg = "data must be a csv file."
                logging.error(msg)
                raise ValueError(msg)
        else:
            msg = "data must be a string"
            logging.error(msg)
            raise ValueError(msg)

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
        logging.info("Starting reading...")
        self.__validate_input_data(data_bank)
        self.__validate_input_data(data_transactions)

        transactions = pd.read_csv(data_transactions, sep=sep)
        bank_dim = pd.read_csv(data_bank, sep=sep)
        self.__validate_columns_read(bank_dim, transactions)

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
        logging.info("Starting transformation...")
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
        """Load data in a csv file and return the same dataframe.

        Parameters
        ----------
        data: pd.Dataframe
        path: str, default = None
            The path to save the dataframe.
        """
        logging.info("Starting loading...")
        if path is not None:
            data.to_csv("RESULT.csv", index=False)
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
        transactions_bank_final = self.__load_data(
            data=transactions_bank, path=path
        )
        logging.info("Pipeline fineshed.")
        return transactions_bank_final
