
from pyspark.sql import SparkSession
from pyspark.sql.functions import col , upper , initcap
from pyarrow.parquet import ParquetFile
import pyarrow as pa 
from    ast import Str
import  pandas as pd
import  datetime
import  json
import  re
import  os

spark = SparkSession                                        \
       .builder                                             \
       .appName("Snowflake_Builder")                        \
       .enableHiveSupport()                                 \
       .config("spark.databricks.io.cache.enabled", "true") \
       .getOrCreate()                   

class Snowflake_Builder():
    '''
    This class is used to standardize build objects in Snowflake so they can be deployed by CICD processes
    This class uses delta objects or file objects to create tables and load them in snowflake
    '''

    def __init__(self,app:str, app_path:str,zone:str, snowflake_db:str, snowflake_schema:str):
        self.app                    = app
        self.app_path               = app_path
        self.zone                   = zone
        self.snowflake_db           = snowflake_db
        self.snowflake_schema    = snowflake_schema



    @property
    def Validate_Parms(self):
         return {'app'                      : self.app,
                 'app_path'                 : self.app_path,
                 'snowflake_db'             : self.snowflake_db,
                 'snowflake_schema'         : self.snowflake_schema,
                 }
        
        

    def Collect_Spark_Table_Meta_Internal(self,excluded_columns:list=['']) :
        Get_Column_MetaData   = udf(lambda x,y: f'{x} {y}', StringType())
        tblList       = sqlContext.tableNames(self.app)
        object_list   = []
        for t in tblList:
            staging_object_name      = f'{self.snowflake_db}.STAGING.{t}'
            object_name              = f'{self.snowflake_db}.{self.snowflake_schema}.{self.app}_{t}'
            loc                      = spark.sql(f"describe detail {self.app}.{t}").collect()[0]["location"]
            df                       = spark.sql(f'''DESCRIBE {self.app}.{t}''')
            df                       = df.withColumn("External_Table_Columns", Get_Column_MetaData('col_name','data_type')) \
                                         .withColumn("Table_Name", upper(lit(object_name))) \
                                         .withColumn("Location", lit(loc))
            
            
            df                       = df.where(~col('col_name').isin(excluded_columns))
            df                       = df.toPandas()[:-5]
            object_list.append(df)
        df                           = pd.concat(object_list)
        df                           = df.groupby(['Table_Name','Location'])['External_Table_Columns'].apply(list).reset_index(name='Column_DDL')
        return df        
    
    def Collect_Spark_Table_Meta_External(self,excluded_columns:list=['']) :
        Get_Column_MetaData   = udf(lambda x,y: f'{x} {y} as (Value:{x}::{y})', StringType())
        tblList       = sqlContext.tableNames(self.app)
        object_list   = []
        for t in tblList:
            staging_object_name      = f'{self.snowflake_db}.STAGING.{t}'
            object_name              = f'{self.snowflake_db}.{self.snowflake_schema}.{self.app}_{t}'
            loc                      = spark.sql(f"describe detail {self.app}.{t}").collect()[0]["location"]
            df                       = spark.sql(f'''DESCRIBE {self.app}.{t}''')
            df                       = df.withColumn("External_Table_Columns", Get_Column_MetaData('col_name','data_type')) \
                                         .withColumn("Table_Name", upper(lit(object_name))) \
                                         .withColumn("Location", lit(loc))
            
            
            df                       = df.where(~col('col_name').isin(excluded_columns))
            df                       = df.toPandas()[:-5]
            object_list.append(df)
        df                           = pd.concat(object_list)
        df                           = df.groupby(['Table_Name','Location'])['External_Table_Columns'].apply(list).reset_index(name='Column_DDL')
        return df
            
    def External_Delta_Table_Pipeline(self, share_name:str, partition_statement:str,external_excluded_columns:list = ['P_Year','P_Month','P_Day'],snowflake_file_format:str = '"PROD_BRONZE_DB"."STAGING"."PARQUET_FORMAT"', cron_schedule:str='CRON 0 3 * * * UTC')->dict:
        df = self.Collect_Spark_Table_Meta_External(excluded_columns=external_excluded_columns)
        '''
        ## Snowflake Public Preview Feature ##
        Snowflake Requires External Delta Table to point to the directory that has the delta log. This function will compile all objects in target app data and convert to equivalent Snowflake create external table statement.
        Snowflake does not support autorefresh or refresh on create on delta tables, it has to be disabled specifically on create external table statement or error is raised when refreshing 
        Steps:
        1- Collect App Objects from Delta DB
        2- Create External Table
        3- Clone File format from Staging if it doesn't exists
        4- Create Secure View for Sharing
        5- Revoke Share so new imprint can be added
        6- Share All app objects with Staples
        7- Refresh Tables and add backup refresh task on Snowflake incase databricks or adf refresh fails
        8- #Remove objects only used to generate cleanup artifacts but is not actually used in pipeline#
        '''

        
        # must use ` intead of single quotes due to the quote cleanup done below those ` will be converted to single quotes before execution #
        df['Column_DDL']                = df.apply(lambda x : str(tuple(x['Column_DDL'])).replace("'","").replace('`',"'"), axis = 1)
        df['search_pattern']            = df.apply(lambda x: re.sub(fr'(.*{self.app}\/(?!.*{self.app}\/))','',x['Location']), axis = 1)
        df['search_pattern']            = df.apply(lambda x: fr'/internal/{self.app}/{x["search_pattern"]}/', axis = 1)

        ## Pipeline Steps ##

        df['CREATE_EXTERNAL_TABLE_DDL'] = df.apply(lambda x : f'''
        DROP Table if EXISTS {x['Table_Name']};
        DROP EXTERNAL Table if EXISTS {x['Table_Name']};
        Create or Replace External Table {x['Table_Name']}
        {x['Column_DDL']}
        {partition_statement}
        WITH LOCATION =  @{self.zone}_{self.snowflake_schema.lower()}{x['search_pattern']}
        FILE_FORMAT   = {self.snowflake_db}.{self.snowflake_schema}.PARQUET_FORMAT_{self.snowflake_schema}
        table_format = delta
        REFRESH_ON_CREATE= false
        auto_refresh = false
                                                                ;
                                                                ''', axis = 1)

        ## there is a bug with external tables that the if the file format is not in the same schema table can't be read over share ##
        df['Add_File_Format']           = df.apply(lambda x: f'''CREATE FILE FORMAT IF NOT EXISTS {self.snowflake_db}.{self.snowflake_schema}.PARQUET_FORMAT_{self.snowflake_schema} CLONE {snowflake_file_format};''',axis =1) 
        df['Create_View']               = df.apply(lambda x: f'''CREATE OR REPLACE SECURE VIEW {x['Table_Name']}_V as Select * from {x['Table_Name']} where Row_IsActive = True;''',axis =1)
        df['revoke_share_data']         = df.apply(lambda x: f'''REVOKE SELECT ON VIEW  {x['Table_Name']}_V FROM SHARE {share_name};''', axis = 1)
        df['share_data']                = df.apply(lambda x: f'''GRANT SELECT ON VIEW   {x['Table_Name']}_V TO SHARE   {share_name};''', axis = 1)
        df['Refresh_ET']                = df.apply(lambda x: f'''
        Alter External Table    {x['Table_Name']} Refresh;
        CREATE OR REPLACE TASK  {x['Table_Name']}_ET_REFRESH warehoudef se=PROD_DATA_ENG_WH schedule= 'USING {cron_schedule}' as alter external table {x['Table_Name']} refresh;
        ''', axis = 1)
        df['Remove_Objects'] = df.apply(lambda x : f'''DROP Table if EXISTS {x['Table_Name']}; DROP EXTERNAL Table if EXISTS {x['Table_Name']};''', axis = 1)

        file_format        = df['Add_File_Format'].head(1).replace('\n','', regex=True).to_string(index=False)
        ets_ddl            = df['CREATE_EXTERNAL_TABLE_DDL'].replace('\n','', regex=True).replace('value string','raw_value string').to_string(index=False)
        create_views       = df['Create_View'].replace('\n','', regex=True).to_string(index=False)
        revoke_share_data  = df['revoke_share_data'].replace('\n','', regex=True).to_string(index=False)
        share_data         = df['share_data'].replace('\n','', regex=True).to_string(index=False)
        auto_refresh       = df['Refresh_ET'].replace('\n','', regex=True).to_string(index=False)
        remove_objects     = df['Remove_Objects'].replace('\n','', regex=True).to_string(index=False)
    
        return {"file_format":file_format,
                "table_ddl":ets_ddl,
                "create_views":create_views,
                "revoke_share_data":revoke_share_data,
                "share_data":share_data,
                "auto_refresh":auto_refresh,
                "remove_objects":remove_objects
               }

    def External_Parquet_Table_Pipeline(self, share_name:str, partition_statement:str,external_excluded_columns:list = ['P_Year','P_Month','P_Day'],snowflake_file_format:str = '"PROD_BRONZE_DB"."STAGING"."PARQUET_FORMAT"', cron_schedule:str='CRON 0 3 * * * UTC')->dict:
        df = self.Collect_Spark_Table_Meta_External(excluded_columns=external_excluded_columns)
        '''
        ## Snowflake GA Feature ##
        Snowflake Requires External Delta Table to point to the directory that has the delta log. This function will compile all objects in target app data and convert to equivalent Snowflake create external table statement.
        Snowflake does not support autorefresh or refresh on create on delta tables, it has to be disabled specifically on create external table statement or error is raised when refreshing 
        Steps:
        1- Collect App Objects from Delta DB
        2- Create External Table
        3- Clone File format from Staging if it doesn't exists
        4- Create Secure View for Sharing
        5- Revoke Share so new imprint can be added
        6- Share All app objects with Staples
        7- Refresh Tables and add backup refresh task on Snowflake incase databricks or adf refresh fails        
        '''

        
        # must use ` intead of single quotes due to the quote cleanup done below those ` will be converted to single quotes before execution #
        df['Column_DDL']                = df.apply(lambda x : str(tuple(x['Column_DDL'])).replace("'","").replace('`',"'"), axis = 1)
        df['search_pattern']            = df.apply(lambda x: re.sub(fr'(.*{self.app}\/(?!.*{self.app}\/))','',x['Location']), axis = 1)
        df['search_pattern']            = df.apply(lambda x: fr'/internal/{self.app}/{x["search_pattern"]}/', axis = 1)

        ## Pipeline Steps ##

        df['CREATE_EXTERNAL_TABLE_DDL'] = df.apply(lambda x : f'''
        DROP Table if EXISTS {x['Table_Name']};
        DROP EXTERNAL Table if EXISTS {x['Table_Name']};
        Create or Replace External Table {x['Table_Name']}
        {x['Column_DDL']}
        {partition_statement}
        WITH LOCATION =  @{self.zone}_{self.snowflake_schema.lower()}{x['search_pattern']}
        FILE_FORMAT   = {self.snowflake_db}.{self.snowflake_schema}.PARQUET_FORMAT_{self.snowflake_schema}
        REFRESH_ON_CREATE= True
        auto_refresh = false
                                                                ;
                                                                ''', axis = 1)

        ## there is a bug with external tables that the if the file format is not in the same schema table can't be read over share ##
        df['Add_File_Format']           = df.apply(lambda x: f'''CREATE FILE FORMAT IF NOT EXISTS {self.snowflake_db}.{self.snowflake_schema}.PARQUET_FORMAT_{self.snowflake_schema} CLONE {snowflake_file_format};''',axis =1) 
        df['Create_View']               = df.apply(lambda x: f'''CREATE OR REPLACE SECURE VIEW {x['Table_Name']}_V as Select * from {x['Table_Name']} where Row_IsActive = True;''',axis =1)
        df['revoke_share_data']         = df.apply(lambda x: f'''REVOKE SELECT ON VIEW  {x['Table_Name']}_V FROM SHARE {share_name};''', axis = 1)
        df['share_data']                = df.apply(lambda x: f'''GRANT SELECT ON VIEW   {x['Table_Name']}_V TO SHARE   {share_name};''', axis = 1)
        df['Refresh_ET']                = df.apply(lambda x: f'''
        Alter External Table    {x['Table_Name']} Refresh;
        CREATE OR REPLACE TASK  {x['Table_Name']}_ET_REFRESH warehoudef se=PROD_DATA_ENG_WH schedule= 'USING {cron_schedule}' as alter external table {x['Table_Name']} refresh;
        ''', axis = 1)
        df['Remove_Objects'] = df.apply(lambda x : f'''DROP Table if EXISTS {x['Table_Name']}; DROP EXTERNAL Table if EXISTS {x['Table_Name']};''', axis = 1)

        file_format        = df['Add_File_Format'].head(1).replace('\n','', regex=True).to_string(index=False)
        ets_ddl            = df['CREATE_EXTERNAL_TABLE_DDL'].replace('\n','', regex=True).replace('value string','raw_value string').to_string(index=False)
        create_views       = df['Create_View'].replace('\n','', regex=True).to_string(index=False)
        revoke_share_data  = df['revoke_share_data'].replace('\n','', regex=True).to_string(index=False)
        share_data         = df['share_data'].replace('\n','', regex=True).to_string(index=False)
        auto_refresh       = df['Refresh_ET'].replace('\n','', regex=True).to_string(index=False)
        remove_objects     = df['Remove_Objects'].replace('\n','', regex=True).to_string(index=False)
    
        return {"file_format":file_format,
                "table_ddl":ets_ddl,
                "create_views":create_views,
                "revoke_share_data":revoke_share_data,
                "share_data":share_data,
                "auto_refresh":auto_refresh,
                "remove_objects":remove_objects
               }

    def Internal_Table_Pipeline(self, share_name:str, partition_statement:str,days_back = -1 ,internal_excluded_columns:list = ['P_Year','P_Month','P_Day'],snowflake_file_format:str = '"PROD_BRONZE_DB"."STAGING"."PARQUET_FORMAT"')->dict:
        processing_date  = date.today()-timedelta(days = -days_back)
        source_year      = processing_date.strftime("%Y")
        source_month     = processing_date.strftime("%m")
        source_day       = processing_date.strftime("%d")
        df = self.Collect_Spark_Table_Meta_Internal(excluded_columns=internal_excluded_columns)
        '''
        ## Snowflake GA Feature ##
        This function will compile all objects in target app data and convert to equivalent Snowflake create external table statement.
        Snowflake does not support autorefresh or refresh on create on delta tables, it has to be disabled specifically on create external table statement or error is raised when refreshing 
        Steps:
        1- Collect App Objects from Delta DB
        2- Create External Table
        3- Clone File format from Staging if it doesn't exists
        4- Create Secure View for Sharing
        5- Revoke Share so new imprint can be added
        6- Share All app objects with Staples
        7- Refresh Tables and add backup refresh task on Snowflake incase databricks or adf refresh fails        
        '''

        
        # must use ` intead of single quotes due to the quote cleanup done below those ` will be converted to single quotes before execution #
        df['Column_DDL']                = df.apply(lambda x : str(tuple(x['Column_DDL'])).replace("'","").replace('`',"'"), axis = 1)
        df['search_pattern']            = df.apply(lambda x: re.sub(fr'(.*{self.app}\/(?!.*{self.app}\/))','',x['Location']), axis = 1)
        df['search_pattern']            = df.apply(lambda x: fr'/internal/{self.app}/{x["search_pattern"]}/P_Year={source_year}/P_Month={source_month}/P_Day={source_day}', axis = 1)

        ## Pipeline Steps ##

        df['CREATE_INTERNAL_TABLE_DDL'] = df.apply(lambda x : f'''
        Create Table IF NOT EXISTS {x['Table_Name']}
        {x['Column_DDL']}
        {partition_statement};
                                                                ''', axis = 1)
        df['load_data']                 = df.apply(lambda x: f'''copy into {x['Table_Name']} from @{self.zone}_{self.snowflake_schema.lower()}{x['search_pattern']};''', axis = 1)
        ## there is a bug with external tables that the if the file format is not in the same schema table can't be read over share ##
        df['Create_View']               = df.apply(lambda x: f'''CREATE OR REPLACE SECURE VIEW {x['Table_Name']}_V as Select * from {x['Table_Name']} where Row_IsActive = True;''',axis =1)
        df['revoke_share_data']         = df.apply(lambda x: f'''REVOKE SELECT ON VIEW  {x['Table_Name']}_V FROM SHARE {share_name};''', axis = 1)
        df['share_data']                = df.apply(lambda x: f'''GRANT SELECT ON VIEW   {x['Table_Name']}_V TO SHARE   {share_name};''', axis = 1)        
        df['Remove_Objects'] = df.apply(lambda x : f'''DROP Table if EXISTS {x['Table_Name']}; DROP EXTERNAL Table if EXISTS {x['Table_Name']};''', axis = 1)


        ets_ddl            = df['CREATE_INTERNAL_TABLE_DDL'].replace('\n','', regex=True).replace('value string','raw_value string').to_string(index=False)
        load_data         = df['load_data'].replace('\n','', regex=True).to_string(index=False)
        create_views       = df['Create_View'].replace('\n','', regex=True).to_string(index=False)
        revoke_share_data  = df['revoke_share_data'].replace('\n','', regex=True).to_string(index=False)
        share_data         = df['share_data'].replace('\n','', regex=True).to_string(index=False)
        remove_objects     = df['Remove_Objects'].replace('\n','', regex=True).to_string(index=False)
    
        return {
                "table_ddl":ets_ddl,
                "load_data":load_data,
                "create_views":create_views,
                "revoke_share_data":revoke_share_data,
                "share_data":share_data,
                "remove_objects":remove_objects
               }