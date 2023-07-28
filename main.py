from src.utils import Pipeline

bank_dim_path = "./src/dataset/bank_dim.csv"
transactions_path = "./src/dataset/transactions.csv"

pipe = Pipeline()
df = pipe.run_pipeline(bank_dim_path, transactions_path, path="./")
print(df)
