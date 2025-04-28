''' Configuration file '''

# Spark parameters
# SPARK_MASTER = 'local[4]'
SPARK_MASTER = 'yarn'
NUM_EXECUTORS = 8

# DB connection parameters
TARGET_DB_NAME = "dp_trans_vsblt_bw"
ARCHIVE_DIR = "/dbfs/mnt/mount_blob/trans0vsblt0bw/prod/archive/"
#STAGING_LOCATION = 'dbfs:/mnt/mount_adls/ap_transfix_tv_na/unrefined'

### Empty archive dir means that we do not archive input excels 
ARCHIVE_DIR_PATH=""

#### TO BE MOVED TO SEPARATE FILE
#### Spark global parameters
SPARK_MASTER = 'yarn'
SPARK_GLOBAL_PARAMS = [
    ('spark.yarn.queue', 'data_load')
]

#### Spark default parameters
SPARK_DEFAULT_PARAMS = [
    ('spark.yarn.queue', 'data_load'),
    ('spark.executor.memory', '5g'),
    ('spark.executor.memoryOverhead', '500m'),
	('spark.sql.broadcastTimeout', "1200000ms")  
]
