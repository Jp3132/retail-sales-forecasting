from extract import Extractor
from transform import Transformer
from load import Loader
from dotenv import load_dotenv
import os
load_dotenv()

class ETLPipeline:

    def __init__(self,api_key:str, series_id:str , db_host:str, db_port:str, db_database:str, db_username:str, db_password:str):
        self.series_id = series_id
        self.extractor = Extractor(api_key)
        self.transformer = Transformer()
        self.loader = Loader(db_host,db_port,db_database,db_username,db_password)

    def run(self):
        try:
            data = self.extractor.extract(self.series_id)
            records = self.transformer.transform(data)
            self.loader.load(records)
            print("ETL Succeeded!")
            
        except Exception as e:
            raise 

if __name__ == "__main__":
    api_key = os.getenv("FRED_API_KEY")
    db_host = os.getenv("DB_HOST")
    db_port = os.getenv("DB_PORT")
    db_database = os.getenv("DB_DATABASE")
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    series_id = "RSXFSN"
    pipeline = ETLPipeline(api_key,series_id,db_host,db_port,db_database,db_username,db_password)
    pipeline.run()



