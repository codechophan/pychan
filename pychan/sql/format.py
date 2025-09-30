from enum import Enum

class Format(Enum):
    Delta = "delta"
    Iceberg = "iceberg"
    Parquet = "parquet"
    Csv = "csv"
    Text = "text"
    Json = "json"
    Excel = "com.crealytics.spark.excel"
    Jdbc = "jdbc"
