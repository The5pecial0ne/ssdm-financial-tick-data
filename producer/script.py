import pandas as pd

df = pd.read_parquet("../answer_key/answer-key-week.parquet")

print(len(df),"rows read from Parquet file")
print(df.head(5))
print(df.columns.tolist())
print(df.dtypes)