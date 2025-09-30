from enum import Enum

class SaveMode(Enum):
    ErrorIfExists = "error"
    Append = "append"
    Overwrite = "overwrite"
    Ignore = "ignore"
