# Docker Databricks Dev Container

`This is a fully self contained docker - poetry project for setting up databricks dev environments`

`All the software required to build this app will be installed when running the steps listed below`

# Prerequisites

## Setup Docker & Git
### Windows
#### Using admin powershell
1. Install WSL if not installed (```wsl --install```)
2. Start WSL (```wsl --list --online```)
3. Add your preferred linux distro (```wsl --install -d Ubuntu-20.04```)
3. Install Chocolatey 

    ```
   Set-ExecutionPolicy Bypass -Scope Process -Force; [System.Net.ServicePointManager]::SecurityProtocol = [System.Net.ServicePointManager]::SecurityProtocol -bor 3072; iex ((New-Object System.Net.WebClient).DownloadString(‘https://chocolatey.org/install.ps1’))
    ```
4. install docker using choco (```choco install -y docker-desktop```)
5. Install git ( if not installed ) (```choco install -y git```)

### Linux
#### Using Bash

```
sudo apt update
sudo apt upgrade
sudo apt install docker.io
sudo usermod -a -G docker $USER
sudo groupadd docker && sudo gpasswd -a ${USER} docker && sudo systemctl restart docker
newgrp docker
sudo systemctl start docker
sudo systemctl enable docker
sudo apt-get install git
```
#
# Tool Usage

1. Clone repo (```git clone repo ```) 
2. go into project directory (``` cd DATABRICKS_CICD ```)
3. Create your feature branch (try to use a relevant branch name) (`git checkout -b feature/fooBar`)
4. Create a databricks.env file ( see sample_databricks_env.txt for format) Add your databricks token to databricks.env file (```DATABRICKS_TOKEN=Enter Your Token Here```)
5. build your local environment (`cd ./build; docker-compose up --build -d`)
6. To load your python scripts into the environment add them to the src folder
7. to run code interactively from terminal run (`docker exec -it (docker inspect --format="{{.Id}}" databricks_cicd) bash`)
#

### Creating Databricks Notebooks Locally

Databricks uses a series of command tags to identify/ differentiate a file from a databricks notebook. Here are the commands that need to be added to your python file for it to be converted to a Notebook.
[More details on importing workbooks](databrick-import-workbooks)

Python : 

- Create a notebook (add to top of the file or cell)

    ```
    # Databricks notebook source
    ```
- Create a code cell

    ```
    # COMMAND ----------
    ```
  
R : 

- Create a notebook (add to top of the file or cell)

    ```
    # Databricks notebook source
    ```
- Create a code cell

    ```
    # COMMAND ----------
    ```

SQL :

- Create a notebook (add to top of the file or cell)

    ```
    -- Databricks notebook source
    ```
- Create a code cell
    ```
    -- COMMAND ----------
    ```

Scala : 

- Create a notebook

    ```
    // Databricks notebook source
    ```
- Create a code cell

    ```
    // COMMAND ----------
    ```
##### Work with dependencies
Typically your main class or Python file will have other dependency JARs and files. You can add such dependency JARs and files by calling sparkContext.addJar("path-to-the-jar") or sparkContext.addPyFile("path-to-the-file")

##### Access DBUtils
You can use dbutils.fs and dbutils.secrets utilities of the Databricks Utilities module. Supported commands are :
```
dbutils.fs.cp, 
dbutils.fs.head,
dbutils.fs.ls, 
dbutils.fs.mkdirs,
dbutils.fs.mv,
dbutils.fs.put,
dbutils.fs.rm,
dbutils.secrets.get,
dbutils.secrets.getBytes,
dbutils.secrets.list,
dbutils.secrets.listScopes
```

##### Copying files between local and remote filesystems


```
from pyspark.dbutils import DBUtils
dbutils = DBUtils(spark)
dbutils.fs.cp('file:/home/user/data.csv', 'dbfs:/uploads')
dbutils.fs.cp('dbfs:/output/results.csv', 'file:/home/user/downloads/')
```

#

### Databricks Connect Overview

 `Please note that databricks connect does not work with existing spark environments to avoid any env contamination this project uses poetry venv to create an isolated work environment.`
 `Must add this option to the clusters advanced config spark.databricks.service.server.enabled true`

Databricks Connect allows you to connect your favorite IDE (Eclipse, IntelliJ, PyCharm, RStudio, Visual Studio Code), notebook server (Jupyter Notebook, Zeppelin), and other custom applications to Databricks clusters.
In the case of this project its meant to enable users to follow CICD processes for deploying code from their local dev machine using Dev Containers (Docker) to elevated environments.
More information can be found about this tool on the following [Microsoft documentation link](https://learn.microsoft.com/en-us/azure/databricks/dev-tools/databricks-connect)

### Basic Usage


#### Deployment Overview
#
![Deployment Solution][deployment-diagram]
#

# Best Practices
### Databricks Medalion Architecture

A medallion architecture is a data design pattern used to logically organize data in a lakehouse, with the goal of incrementally and progressively improving the structure and quality of data as it flows through each layer of the architecture (from Bronze ⇒ Silver ⇒ Gold layer tables). Medallion architectures are sometimes also referred to as "multi-hop" architectures.
[Databricks documentation](https://www.databricks.com/glossary/medallion-architecture#:~:text=A%20medallion%20architecture%20is%20a%20data%20design%20pattern,%28from%20Bronze%20%E2%87%92%20Silver%20%E2%87%92%20Gold%20layer%20tables%29.)

#
# Sample Script 
Example Script built with this project to test execution
```
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from datetime import date

spark = SparkSession.builder.appName('temps-demo').getOrCreate()

# Create a Spark DataFrame consisting of high and low temperatures
# by airport code and date.
schema = StructType([
    StructField('AirportCode', StringType(), False),
    StructField('Date', DateType(), False),
    StructField('TempHighF', IntegerType(), False),
    StructField('TempLowF', IntegerType(), False)
])

data = [
    [ 'BLI', date(2021, 4, 3), 52, 43],
    [ 'BLI', date(2021, 4, 2), 50, 38],
    [ 'BLI', date(2021, 4, 1), 52, 41],
    [ 'PDX', date(2021, 4, 3), 64, 45],
    [ 'PDX', date(2021, 4, 2), 61, 41],
    [ 'PDX', date(2021, 4, 1), 66, 39],
    [ 'SEA', date(2021, 4, 3), 57, 43],
    [ 'SEA', date(2021, 4, 2), 54, 39],
    [ 'SEA', date(2021, 4, 1), 56, 41]
]

temps = spark.createDataFrame(data, schema)

# Create a table on the Databricks cluster and then fill
# the table with the DataFrame's contents.
# If the table already exists from a previous run,
# delete it first.
spark.sql('USE default')
spark.sql('DROP TABLE IF EXISTS demo_temps_table')
temps.write.saveAsTable('demo_temps_table')

# Query the table on the Databricks cluster, returning rows
# where the airport code is not BLI and the date is later
# than 2021-04-01. Group the results and order by high
# temperature in descending order.
df_temps = spark.sql("SELECT * FROM demo_temps_table " \
    "WHERE AirportCode != 'BLI' AND Date > '2021-04-01' " \
    "GROUP BY AirportCode, Date, TempHighF, TempLowF " \
    "ORDER BY TempHighF DESC")
df_temps.show()

# Results:
#
# +-----------+----------+---------+--------+
# |AirportCode|      Date|TempHighF|TempLowF|
# +-----------+----------+---------+--------+
# |        PDX|2021-04-03|       64|      45|
# |        PDX|2021-04-02|       61|      41|
# |        SEA|2021-04-03|       57|      43|
# |        SEA|2021-04-02|       54|      39|
# +-----------+----------+---------+--------+

# Clean up by deleting the table from the Databricks cluster.
spark.sql('DROP TABLE demo_temps_table')
```
#
## Contributing

1. Fork it 
2. Create your feature branch (`git checkout -b feature/fooBar`)
3. Commit your changes (`git commit -am 'Add some fooBar'`)
4. Push to the branch (`git push origin feature/fooBar`)
5. Create a new Pull Request

 [More Details on Forking a repo](forking-repo)

### Technologies used by this build 
#
[More Details on Chocolatey](choco)

[More Details on Docker](docker)



[More Details on Poetry](poetry)
<!-- Markdown link & img dfn's -->
[forking-repo]: https://docs.microsoft.com/en-us/azure/devops/repos/git/forks?view=azure-devops&tabs=visual-studio-2019
[choco]: https://community.chocolatey.org/courses/getting-started/what-is-chocolatey
[docker]: https://learn.microsoft.com/en-us/dotnet/architecture/microservices/container-docker-introduction/docker-defined
[poetry]: https://python-poetry.org/docs/
[deployment-diagram]: https://lucid.app/publicSegments/view/714776b6-8cb5-4280-9957-ee1300c696d1/image.png
[databrick-import-workbooks]: https://learn.microsoft.com/en-us/azure/databricks/notebooks/notebook-export-import

#
`cleanup docker env`
#
```
docker kill $(docker container ls -q)
docker system prune -a 
``` 