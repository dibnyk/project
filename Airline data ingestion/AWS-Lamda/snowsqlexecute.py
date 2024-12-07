import snowflake.connector as sf
from snowflake.connector import connect, DictCursor
import boto3
import re

logger = load_log_config()
S3C = boto3.client('s3')
S3R = boto3.resource('s3')


def inputs(event_data):
    """
    Function to get params from cloudwatch event
    """

    s3_bucket=event_data['s3_bucket']
    code_key= event_data['code_key']
    warehouse_size= event_data['warehouse_size']
    db = event_data['sf_db']
    schema = event_data['sf_schema']

    return s3_bucket, code_key, warehouse_size, db, schema


def snowflake_connection(warehouse_size_sfx,database, schema):
    """
    Function to establish snowflake connection
    :param warehouse_size_sfx:
    :return:
    """
    try:
        ctx = sf.connect(
            user="<your_snowflake_user>",
            password="<your_snowflake_password>",
            account="<your_snowflake_account>",
            warehouse="<your_snowflake_warehouse>",
            database="<your_snowflake_database>",
            schema="<your_snowflake_schema>"
        )
        cur = ctx.cursor(DictCursor)
        cur.execute('ALTER SESSION SET TRANSACTION_ABORT_ON_ERROR = TRUE')
        cur.execute('ALTER SESSION SET QUERY TAG = LOAD')
    except sf.DatabaseError as SnowflakeConnectionFailure:
        logger.critical("CRITICAL: Cannot establish connection")
        logger.Critical(SnowflakeConnectionFailure)
        raise SnowflakeConnectionFailure
    logger.info("Snowflake Connection established successfully")

    return cur


def sql_file(s3_sql_bucket, s3_sql_key):
    """
    Function to get sql files from s3 and read it for execution
    :param s3_sql_bucket: s3 bucket for sql files
    :param s3_sql_key: code file path in s3
    :return:
    """

    s3_sql_key = S3R.Bucket(s3_sql_bucket)
    s3_sql_key = s3_sql_key

    sql_file_list = []

    for sql_file in s3_sql_bucket.objects.filter(Prefix=s3_sql_key):
        sql_file_list.append(sql_file.key)

    logger.info('Files to be run: %s', sql_file_list)

    if len(sql_file_list) == 0:
        raise InvalidStatus(f"No file found in path - {s3_sql_bucket}/{s3_sql_key}")

    # getting the file
    for file in sql_file_list:
        sql_obj = S3C.get_object(
            Bucket=s3_sql_bucket,
            Key=file
        )

    #reading the file
    logger.info('Reading file',file)
    sql = sql_obj['Body'].read()
    sql_query = sql.decode('utf-8')

    # to store the sql queries
    sql_list = []
    sql_list = sql_query.split(";")

    return sql_list


def snowflake(sql_list,cur):
    """
    Executes sqls one by one
    :param sql_list: List of sql queries
    :param cur: Snowflake cursor
    """

    sql_queries = len(sql_list)
    logger.info('Total Number of sqls to excute: %s',str(sql_queries))

    try:

        logger.info('Executing Snowqsql script')
        cur.execute("BEGIN")

        for sqls in sql_list:
            if sqls != '' and sqls is not None and sqls !='\n\n':
                cur.execute(sqls)

        cur.execute("COMMIT")
        logger.info('File is complete. Execution Successful')

    except sf.ProgrammingError as GenericSnowflakeException:
        cur.execute("ROLLBACK")
        logger.critical('Error: Snoflake execution failed: %s',sqls)
        logger.Critical(GenericSnowflakeException)
        raise GenericSnowflakeException

    return True


def lambda_handler(event, context):
    """
    Function that conatins the whole Flow. Establishes snowflake connection and executes sqls
    """

    try:
        s3_bucket, code_key, warehouse_size, db, schema = inputs(event)
        cur = snowflake_connection(warehouse_size,db, schema)
        sql_list = sql_file(s3_bucket, code_key)
        snowflake(sql_list, cur)

    except Exception as error:
        logger.error('Uncaught error while executing flow. Error Msg: {0}'.format(error))
        raise