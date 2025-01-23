import pandas as pd
import os

with open('data_preprocessing.txt', 'w') as f:
    for file_path in os.listdir('ecommerce_dataset'):

        if file_path.endswith('.csv'):
            data_set_name = file_path.split('.')[0]
            df = pd.read_csv(f'ecommerce_dataset/{file_path}')
            f.write(f'Dataset Name: {data_set_name}\n')
            f.write(f'Number of rows: {df.shape[0]}\n')
            f.write(f'Number of columns: {df.shape[1]}\n')
            f.write(f'Columns: {df.columns.to_list()}\n')
            f.write(f'Columns data types: {df.dtypes.to_list()}\n')
            f.write(f'Number of null values: {df.isnull().sum().sum()}\n')
            f.write(f'Number of duplicates: {df.duplicated().sum()}\n')
            f.write(f'Number of unique values for each column: {df.nunique()}\n')
