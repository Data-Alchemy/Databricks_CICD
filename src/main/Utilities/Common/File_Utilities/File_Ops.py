
from pyspark.sql                import SparkSession
from datetime                   import date, timedelta , datetime
from pyspark.sql.functions      import *
from pyspark.sql.types          import IntegerType,StringType
from pyspark.sql 				import DataFrame as SparkDataFrame


def recursive_list(path:str)->list:
    '''
    Loops over objects in a given path to return a list of all files in a specified location 
    '''
    file_list = []
    dir_paths = dbutils.fs.ls(path)
    counter=0
    while dir_paths:
        cursor = dir_paths.pop()
        if cursor.isDir()== False:
            file_list.append(cursor.path)
        if cursor.isDir():
            dir_paths += dbutils.fs.ls(cursor.path)
    return file_list

def df_partition(df:SparkDataFrame,rowsPerPartition:int=1000000):
    '''
    This function reads a dataframe and returns optimal partition size for writting data to blob storage default value is 1,000,000 records
    I have found this to be a decent file size for most cases 
    '''
    partitions = int(1 + df.count() / rowsPerPartition)
    return partitions



def Cleanup(database_name:str , table_name:str):

    '''
    # Will Delete everything from specified location in Storage account and Delta Tables associated to that location #
    usage pass name of delta database and table like so Cleanup(database_name,table_name)
    '''

    loc = spark.sql(f'''Describe Detail {database_name}.{table_name}''').collect()[0]['location']
    spark.sql(f'''Drop table {database_name}.{table_name}''')
    dbutils.fs.rm(loc,recurse= True)


'''
UDF Functions used to parse file name attributes from path
File Name uses negative look ahead in regex based on posix date format
'''

File_Name        = udf(lambda x: re.sub('(.*[0-9]{4}\/[0-9]{2}\/[0-9]{2}\/(?!.*[0-9]{4}\/[0-9]{2}\/[0-9]{2}\/))','',x))
File_Date        = udf(lambda x: re.sub("P_\d{5}|[^\d]","", x)[:8], StringType())
File_Year        = udf(lambda x: x[:4], StringType())
File_Month       = udf(lambda x: x[4:6], StringType())
File_Day         = udf(lambda x: x[6:], StringType())


'''
Common Date Values used as literals or operators in pySpark
'''
currentSecond   = datetime.now().second
currentMinute   = datetime.now().minute
currentHour     = datetime.now().hour
currentDay      = int(datetime.now().day)
currentMonth    = str(datetime.now().month)
currentYear     = int(datetime.now().year)