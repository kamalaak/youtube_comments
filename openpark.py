import pandas as pd

all_dfs = []

for i in range(10):
    df = pd.read_parquet(f'./comment_data/batch_{i+1}.parquet')
    all_dfs.append(df)

# Concatenate all DataFrames
final_df = pd.concat(all_dfs, ignore_index=True)

# Save to CSV once
final_df.to_csv('code_mixed_comments.csv', index=False)

# View the first few rows
print(final_df.head())
