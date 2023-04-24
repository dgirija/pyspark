'''https://www.youtube.com/watch?v=EttGKeT4e3U&t=2867s'''


import logging
import json, os, re, sys
from typing import Callable, Optional
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession


#! current path name
project_dir = (os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
LOG_FILE = f'{project_dir}/logs/job-{os.path.basename(__file__)}.log'
LOG_FORMAT = f"%(asctime)s - LINE:%(lineno)d - %(name)s - %(levelname)s - %(funcName)s - %(message)s"
logging.basicConfig(filename=LOG_FILE, level=logging.DEBUG, format=LOG_FORMAT)
logger = logging.getLogger('py4j')


sys.path.insert(1,project_dir)
from classes import class_pyspark


def main(project_dir:str) -> None:
    '''Start A spark job'''
    # class_pyspark.Sparkclass(config = {}).sparkStart()
    conf = openFile(f"{project_dir}/json/sales.json")
    spark = sparkStart(conf)

    transactionDf = importData(spark,f"{project_dir}/test-data/sales/transactions",".json$")

    sparkStop(spark)


def openFile(filepath: str) -> dict:
    def openJson(filepath:str) -> dict:
        if isinstance(filepath,str) and os.path.exists(filepath):
            with open(filepath, "r") as f:
                data = json.load(f)
            return data
    return (openJson(filepath))


def sparkStart(conf:dict) -> SparkSession:
    if isinstance(conf, dict):
        return class_pyspark.Sparkclass(config={}).sparkStart(conf)


def sparkStop(spark:SparkSession) -> None:
    spark.stop() if isinstance(spark,SparkSession) else None


def importData(spark:SparkSession, datapath:str,pattern:Optional[str]=None) -> DataFrame:
    if isinstance(spark,SparkSession):
        return class_pyspark.Sparkclass(config={}).importData(spark,datapath,pattern)

if __name__ == '__main__':
    main(project_dir)