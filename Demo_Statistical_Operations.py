import numpy as np
import pandas as pd

products_df = pd.read_csv ("C:\\Users\\rohit\\Spark_Learning\\input_data\\starter\\products.csv" , header=0)
print(products_df.head(5))
print(products_df.product_category.value_counts())
