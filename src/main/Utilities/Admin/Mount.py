# Databricks notebook source


from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()

sc = spark.sparkContext
from pyspark.sql import SparkSession
from dbruntime.dbutils import FileInfo


dbutils.fs.refreshMounts()
class ADLS_Mounts():

    def __init__(self, zones        :list           = None,
                 subject_areas      :list           = None, 
                 tenant_id          :str            = '<enter tenant>',
                 client_id          :str            = None,
                 client_secret      :str            = None,
                 storage_endpoint   :str            = None   
                ):
      
        self.zones              = zones
        self.subject_areas      = subject_areas
        self.tenant_id          = tenant_id
        self.client_id          = client_id
        self.client_secret      = client_secret
        self.storage_endpoint   = storage_endpoint 

    @property
    def getkeys(self)->dict:
        if self.client_id == None or self.client_secret == None:
            #print("No credential provided will use credentials from keyvault")
            try:
                gold_id             = dbutils.secrets.get("<enter key vault name>", "KV-AAD-app-DataLake-Gold-clientID")
                gold_credential     = dbutils.secrets.get("<enter key vault name>", "KV-AAD-app-DataLake-Gold-secret")
                silver_id           = dbutils.secrets.get("<enter key vault name>", "KV-AAD-app-DataLake-Silver-clientID")
                silver_credential   = dbutils.secrets.get("<enter key vault name>", "KV-AAD-app-DataLake-Silver-secret")
                bronze_id           = dbutils.secrets.get("<enter key vault name>", "KV-AAD-app-DataLake-Bronze-clientID")
                bronze_credential   = dbutils.secrets.get("<enter key vault name>", "KV-AAD-app-DataLake-Bronze-secret")
                return                 {"dlgold" :{'client_id':gold_id      ,   'client_secret':gold_credential},
                                        "dlsilver":{'client_id':silver_id   ,   'client_secret':silver_credential},
                                        "dlbronze":{'client_id':bronze_id   ,   'client_secret':bronze_credential}
                                        }
            except Exception as e:
                print("unable to retrieve secrets from keyvault please check with infra team \nexception is:",e)
        else:
            print("credential recieved using provided credentials")
            return {f'{self.zones[0]}':{'client_id':self.client_id,'client_secret':self.client_secret}}


    @property
    def create_mount(self):
        if self.zones == None :
            self.zones = ['dlgold', 'dlsilver','dlbronze']
        else: 
          self.zones 
        if self.subject_areas == None :
            self.subject_areas = ['application-data','master-data', "retail-data", "people-data" , "supply-chain-data" ]
        else: 
          self.subject_areas
        for zone in self.zones:
          zone_client_id = self.getkeys[zone]['client_id']
          zone_client_secret = self.getkeys[zone]['client_secret']
          for sub in self.subject_areas:
            mount_path = "abfss://" + sub + "@" + zone + self.storage_endpoint +".dfs.core.windows.net/"
            mount_name = f"/mnt/adls_{zone}_{sub}"
            configs = {"fs.azure.account.auth.type": "OAuth",
                           "fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
                           "fs.azure.account.oauth2.client.id": zone_client_id,
                           "fs.azure.account.oauth2.client.secret":  zone_client_secret,
                           "fs.azure.account.oauth2.client.endpoint": f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/token"}
            i = [item.mountPoint for item in dbutils.fs.mounts()].count(mount_name)
            print(mount_name)

            if i >0 :
              dbutils.fs.unmount(mount_name)
              print(dbutils.fs.mount(source=mount_path,mount_point=mount_name, extra_configs=configs))

            else :
                
              print(dbutils.fs.mount(source=mount_path,mount_point=mount_name, extra_configs=configs))