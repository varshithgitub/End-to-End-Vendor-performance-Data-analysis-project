import pandas as pd
import os 
import time
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import logging


logging.basicConfig(
    filename ="logs/ingestion_db.log",
    level = logging.DEBUG,
    format="%(asctime)s - %(levelname)s - %(message)s",
    filemode="a"
)

engine = create_engine('sqlite:///inventory.db')

def ingest_db(df, table_name, engine):
    df.to_sql(table_name, con = engine, if_exists = 'replace', index = False)

    
def load_rawdata(): 
    #this fuction loads csv as dataframe and ingest into db 
    start = time.time()
    for file in os.listdir('data'):
        if '.csv' in file:
            df = pd.read_csv('data/'+file)
            logging.info(f'Ingestion in progress for {file} in DB')
            ingest_db(df,file[:-4], engine)
    end = time.time()
    t_t = (end - start)/60
    logging.info(f'Ingestion success !!! Total time take is {t_t} minutes')



if __name__== '__main__':
    load_rawdata()