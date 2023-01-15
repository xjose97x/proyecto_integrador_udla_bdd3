from config import StagingProperties, CoreProperties
from extract import truncate, extract, tests as extract_tests
from transform import transform, tests as transform_tests
from load import load, tests as load_tests
from utils.db_connection import Db_Connection
from utils import query_utils


staging_db_context = Db_Connection(
    'mysql', StagingProperties.URL, StagingProperties.PORT, StagingProperties.USER,
    StagingProperties.PASSWORD, StagingProperties.NAME).start()

core_db_context = Db_Connection(
    'mysql', CoreProperties.URL, CoreProperties.PORT, CoreProperties.USER,
    CoreProperties.PASSWORD, CoreProperties.NAME).start()

etl_process_id = query_utils.generate_etl_process_id(staging_db_context)
print(f'ETL Process ID: {etl_process_id}')

with staging_db_context.begin():  # transaction
    truncate(staging_db_context)
    extract(staging_db_context)
    transform(staging_db_context, etl_process_id)


with core_db_context.begin():  # transaction
    load(staging_db_context, core_db_context, etl_process_id)


extract_tests(staging_db_context)
transform_tests(staging_db_context, etl_process_id)
load_tests(core_db_context)


staging_db_context.dispose()
core_db_context.dispose()
