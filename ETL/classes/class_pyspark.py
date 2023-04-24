import json, os, re, sys
from typing import Callable, Optional
from pyspark.sql.dataframe import DataFrame
from pyspark.sql import SparkSession
from pyspark import SparkContext


class Sparkclass:
    def __init__(self,conf):
        self.conf = conf

    def sparkStart(self, kwargs:dict):
        MASTER = kwargs['spark_conf']['master']
        APP_NAME = kwargs['spark_conf']['app_name']
        LOG_LEVEL = kwargs['log']['level']

        def createSession(master:Optional[str]="local[*]",app_name:Optional[str]="myapp") -> SparkSession:
            ''' Create a spark session '''
            spark = SparkSession.builder.appName(app_name).master(master).getOrCreate()
            return spark
        
        def setLogging(spark:SparkSession, log_level:Optional[str] = None) -> None:
            spark.sparkContext.setLogLevel(log_level) if isinstance(log_level,str) else None

        def getSettings(spark:SparkSession) -> None:
            ''' Show spark setting'''
            print(f"\033[1;33m{spark}\033[0m")
            print(f"\033[96m{spark.sparkContext.getConf().getAll()}\033[0m")
            pass
        spark = createSession(MASTER,APP_NAME)
        setLogging(spark, LOG_LEVEL)
        getSettings(spark)

        return spark