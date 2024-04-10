import shutil
from datetime import datetime
from fileinput import filename
import pandas as pd
import spark
from pyspark.sql.functions import *
from pyspark.sql.connect.functions import lit, concat_ws,col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType, DateType
from resources.dev import config
from src.main.move.move_files import move_s3_to_s3
from src.main.read.database_read import DatabaseReader
from src.main.utility.s3_client_object import *
from src.main.utility.encrypt_decrypt import *
from src.main.utility.logging_config import *
import os
from src.main.utility.my_sql_session import *
from src.main.read.aws_read import *
from src.main.download.aws_file_download import S3FileDownloader
from datetime import datetime, timedelta
from py4j.java_gateway import java_import
from src.main.utility.spark_session import spark_session



######## GET S3 CLIENT############
aws_access_key = config.aws_access_key
aws_secret_key = config.aws_secret_key


s3_client_provider = S3ClientProvider(decrypt(aws_access_key),decrypt(aws_secret_key))
s3_client = s3_client_provider.get_client()

response = s3_client.list_buckets()
logger.info("List of Buckets: %s",response['Buckets'])


# check if local dir has already a file
# if if file is there then check if the same file is present in the staging area
# with status as A. If so then do not delete and try to re-run
# else give an error and not process the next file

csv_files = [file for file in os.listdir(config.local_directory) if file.endswith(".CSV")]
connection = get_mysql_connection()
cursor = connection.cursor()

total_csv_files = []
if csv_files:
    for file in csv_files:
        total_csv_files.append(file)

    statement = f"select distinct file_name from " \
                f"datamart.product_staging_table " \
                f"where file_name in ({str(total_csv_files)[1:1]}) and status= 'A' "
    logger.info(f"dynamically statement created: {statement} ")
    cursor.execute(statement)
    data = cursor.fetchall()

    if data:
        logger.info("Your last run was failed please check")
    else:
        logger.info("no report match")

else:
    logger.info("Last run was successful!!!")



try:
    s3_reader = S3Reader()
    #Bucket name should come from table
    folder_path = config.s3_source_directory
    s3_absolute_file_path = s3_reader.list_files(s3_client,
                                                 config.bucket_name,
                                                 folder_path=folder_path)
    logger.info("Absolute path on s3 bucket for csv file %s",s3_absolute_file_path)

    if not s3_absolute_file_path:
        logger.info(f"No File available at {folder_path}")
        raise Exception("No data available to process ")

except Exception as e:
    logger.error("Exited with error:- %s", e)
    raise e


bucket_name = config.bucket_name
local_directory = config.local_directory

perfix = f"s3://{bucket_name}/"
file_paths = [url[len(perfix):] for url in s3_absolute_file_path]
logging.info("File path available on s3 under %s bucket and folder name is %s", bucket_name,file_paths)
logging.info(f"File path available on s3 under {bucket_name} bucket and folder name is {file_paths}")

try:
    downloader = S3FileDownloader(s3_client,bucket_name,local_directory)
    downloader.download_files(file_paths)
except Exception as e:
    logger.error("File downlaod error: %s",e)
    sys.exit()

#get a list of all file in local directory
all_files = os.listdir(local_directory)
logger.info(f"list of file present in local dir after download {all_files}")

# filter files with ".csv" in their names and create absolute paths

if all_files:
    csv_files = []
    error_files =[]
    for files in  all_files:
        if files.endswith(".csv"):
            csv_files.append(os.path.abspath(os.path.join(local_directory,files)))
        else:
            error_files.append(os.path.abspath(os.path.join(local_directory,files)))

    if not csv_files:
        logger.error("No csv data available to process the request")
        raise Exception ("No csv data available to process the request")

else:
    logger.error("There is no data to process")
    raise Exception("There is no data to process")

# make csv file convert into a list
logger.info("**************listing the file******")
logger.info("list of csv files that needs to be processed %s",csv_files)

logger.info("**************creating spark session*****")

spark = spark_session()

logger.info("********spark session created *********")

# # check the required column of the schema in the csv
# # if not required columns keep it in a list error_files
# # else union all the data into one dataframe

logger.info("checking the schema ")

correct_files = []
for data in csv_files:
    data_schema = spark.read.format("csv")\
                  .option("header","true")\
                  .load(data).columns
    logger.info(f"Schema for the {data} is {data_schema}")
    logger.info(f"Mandatory columns schema is {config.mandatory_columns}")
    missing_columns = set(config.mandatory_columns) - set(data_schema)
    logger.info(f"missing columns are {missing_columns}")

    if missing_columns:
        error_files.append(data)
    else:
        logger.info(f"No missing coluimn for the {data}")
        correct_files.append(data)


logger.info(f"*****list of correct file*****{correct_files}")
logger.info(f"*****list of error file*****{error_files}")
logger.info(f"*****moving error file to error dir if any*****")

# # move the data to error dir

error_folder_local_path = config.error_folder_path_local
for file_path in error_files:
    if os.path.exists(file_path):
        file_name = os.path.basename(file_path)
        destination_path = os.path.join(error_folder_local_path,file_name)


        shutil.move(file_path,destination_path)
        logger.info(f"Moved '{file_name}' from s3 file path to '{destination_path}")

        source_prefix = config.s3_source_directory
        destination_prefix = config.s3_error_directory

        message = move_s3_to_s3(s3_client,config.bucket_name,source_prefix,destination_prefix,file_name)
        logger.info(f"{message}")
    else:
        logger.error(f"'{file_path}' does not exist.")
else:
    logger.info("************there is no error files available at our dataset******")


# additional column needs to be taken care of determine extra colimns
# before running the process
# stage table needs to be updated with status as actiave (A) or inactiave (I)

logger.info(f"*****************updating the product_staging_table that we have started the process***")
insert_statements = []
db_name = config.database_name
current_date = datetime.now()
formatted_date = current_date.strftime("%Y-%m-%d %H:%M:%S")
if correct_files:
    for file in correct_files:
        file_name = os.path.basename(file)
        statements=f"INSERT INTO {db_name}.{config.product_staging_table} " \
                   f"(file_name, file_location,created_date,status)" \
                   f"VALUES ('{filename}','{filename}','{formatted_date}','A')"

        insert_statements.append(statements)

    logger.info(f"insert statement created for staging table ----{insert_statements}")
    logger.info("**************connecting with my sql server**********")
    connection = get_mysql_connection()
    cursor = connection.cursor()
    logger.info("**********my sql server connected succesfully")
    for statement in insert_statements:
        cursor.execute(statement)
        connection.commit()
    cursor.close()
    connection.close()

else:
    logger.error("********************there is no files to process ")
    raise Exception("*********No data available with correct files ********")


logger.info("******************fixing extra column coming from source")
logger.info("****************fixing extra column coming from source*********")

schema = StructType([
    StructField("customer_id",IntegerType() ,True),
    StructField("store_id",IntegerType() ,True),
    StructField("product_name",StringType() ,True),
    StructField("sales_date",DateType(),True),
    StructField("sales_person_id",IntegerType(),True),
    StructField("price",FloatType(),True),
    StructField("quantity",IntegerType(),True),
    StructField("total_cost",FloatType(),True),
    StructField("additional_column",StringType(),True)

])

# database_client = DatabaseReader(config.url,config.properties)
logger.info("****************creating empty dataframe*********")

# final_df_to_process = database_client.create_dataframe(spark,"empty_df_create_table")

final_df_to_process = spark.createDataFrame([],schema=schema)
final_df_to_process.show()
# create a new column with concatinated values of extra columns

for data in correct_files:
    data_df = spark.read.format("csv") \
            .option("header",'True') \
            .option("inferSchem",'True') \
            .load(data)

    data_schema = data_df.columns
    extra_columns = list(set(data_schema) - set(config.mandatory_columns))
    logger.info(f"extra columns present at source at source is {extra_columns}")
    if extra_columns:
        data_df = data_df.withColumn("additional_column",concat_ws(",",col(*extra_columns)))\
                  .select("customer_id","store_id","product_name","sales_date","sales_person_id",
                          "price","quantity","total_cost","additional_column")
        logger.info(f"processed {data} and added 'additional_columns'")
    else:
        data_df = data_df.withColumn("additional_column", lit(None).cast("string"))\
            .select("customer_id","store_id", "product_name","sales_date","sales_person_id",
                     "price","quantity","total_cost","additional_column")

    final_df_to_process=final_df_to_process.union(data_df)

logger.info("************final dataframe from source will be going to******** ")
final_df_to_process.show()
