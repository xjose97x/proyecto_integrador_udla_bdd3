from dotenv import dotenv_values

core_properties = dotenv_values("./config/core.properties")
staging_properties = dotenv_values("./config/staging.properties")
data_properties = dotenv_values("./config/data.properties")

class CoreProperties:
    URL = core_properties["DB_URL"]
    PORT = core_properties["DB_PORT"]
    USER = core_properties["DB_USER"]
    PASSWORD = core_properties["DB_PASSWORD"]
    NAME = core_properties["DB_NAME"]

class StagingProperties:
    URL = staging_properties["DB_URL"]
    PORT = staging_properties["DB_PORT"]
    USER = staging_properties["DB_USER"]
    PASSWORD = staging_properties["DB_PASSWORD"]
    NAME = staging_properties["DB_NAME"]

class DataProperties:
    DATA_PATH = data_properties["DATA_PATH"]