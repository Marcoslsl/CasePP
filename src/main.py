from src.utils import Pipeline

bank_dim_path = "./dataset/bank_dim.csv"
transactions_path = "./dataset/transactions.csv"

pipe = Pipeline()
df = pipe.run_pipeline(bank_dim_path, transactions_path)
print(df)
