import pandas as pd

class Transformer:
    def __init__(self):
        pass

    def transform(self,data):
        try:

            df = pd.DataFrame(data,columns=['sales_amount'])
            df['sales_amount'] = df['sales_amount'].map(lambda sales_amount: sales_amount/1000)
            df.index.freq = "MS"
            sales_tuples = [(index.date(),float(sales_amount)) for index,sales_amount in df.itertuples()]
            return sales_tuples
        
        except Exception as e:
            raise e
