from pyspark.sql.session import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.readwriter import (
    DataFrameReader,
    DataFrameWriter
)
from pyspark.sql.types import StructType

from pychan.sql.format import Format
from pychan.sql.mode import SaveMode
from pychan.sql.merge import MergeIntoWriter
from pychan.types import (
    String,
    Map,
    List,
    Optional,
    Unit
)

class Dataset:
    def __init__(
        self,
        spark: SparkSession
    ) -> Unit:
        self._spark: SparkSession = spark
    
    def readDF(
        self,
        path: Optional[String] = None,
        format: Optional[Format | String] = None,
        schema: Optional[StructType | String] = None,
        options: Optional[Map[String, String]] = None
    ) -> DataFrame:
        """
        >>> ds.readDF(
        ...     path = "Files/...",
        ...     format = Format.Json,
        ...     schema = (
        ...         StructType([
        ...             StructField("organization", StringType(), True),
        ...             StructField("website", StringType(), True)
        ...         ])
        ...     )
        ... )
        
        >>> ds.readDF(
        ...     path = "Files/...",
        ...     format = Format.Csv,
        ...     options = {
        ...         "header": "true",
        ...         "nullValue": "",
        ...         "inferSchema": "true"
        ...     }
        ... )
        
        >>> ds.readDF(
        ...     path = "Files/...",
        ...     format = Format.Parquet
        ... )
        """
        reader: DataFrameReader = self._spark.read
        
        if path:
            reader = reader.option("path", path)
        
        if format:
            reader = reader.format(format.value)
        
        if schema:
            reader = reader.schema(schema)
        
        if options:
            reader = reader.options(**options)
        
        return reader.load()
    
    def readParquet(
        self,
        path: String,
        options: Optional[Map[String, String]] = None
    ) -> DataFrame:
        """
        >>> ds.readParquet(
        ...     path = "Files/...",
        ...     options = {
        ...         "mergeSchema", "true"
        ...     }
        ... )
        """
        reader: DataFrameReader = self._spark.read.format(Format.Parquet.value)
        
        if options:
            reader = reader.options(**options)
        
        return reader.load(path)
    
    def readCsv(
        self,
        path: String,
        schema: Optional[StructType | String] = None,
        options: Optional[Map[String, String]] = None
    ) -> DataFrame:
        """
        >>> ds.readCsv(
        ...     path = "Files/...",
        ...     options = {
        ...         "header": "true",
        ...         "nullValue": "",
        ...         "inferSchema": "true"
        ...     }
        ... )
        """
        reader: DataFrameReader = self._spark.read.format(Format.Csv.value)
        
        if schema:
            reader = reader.schema(schema)
        
        if options:
            reader = reader.options(**options)
        
        return reader.load(path)
    
    def readJson(
        self,
        path: Optional[String] = None,
        schema: Optional[StructType | String] = None,
        options: Optional[Map[String, String]] = None
    ) -> DataFrame:
        """
        >>> ds.readJson(
        ...     path = "Files/...",
        ...     schema = (
        ...         StructType([
        ...             StructField("organization", StringType(), True),
        ...             StructField("website", StringType(), True)
        ...         ])
        ...     )
        ... )
        """
        reader: DataFrameReader = self._spark.read.format(Format.Json.value)
        
        if schema:
            reader = reader.schema(schema)
        
        if options:
            reader = reader.options(**options)
        
        return reader.load(path)
    
    def writeDF(
        self,
        df: DataFrame,
        path: String,
        format: Optional[Format | String] = None,
        mode: Optional[SaveMode | String] = None,
        partitionColsName: Optional[List[String]] = None,
        options: Optional[Map[String, String]] = None
    ) -> Unit:
        """
        >>> ds.writeDF(
        ...     df = df,
        ...     path = "Tables/Order",
        ...     format = Format.Delta,
        ...     mode = SaveMode.Overwrite,
        ...     options = {
        ...         "overwriteSchema": "true"
        ...     }
        ... )
        """
        writer: DataFrameWriter = df.write
        
        if format:
            writer = writer.format(format.value)
        if mode:
            writer = writer.mode(mode.value)
        if partitionColsName:
            writer = writer.partitionBy(*partitionColsName)
        if options:
            writer = writer.options(**options)
        
        writer.save(path)
    
    def writeTable(
        self,
        df: DataFrame,
        tblName: String,
        format: Optional[Format | String] = None,
        mode: Optional[SaveMode | String] = None,
        partitionColsName: Optional[List[String]] = None,
        options: Optional[Map[String, String]] = None
    ) -> Unit:
        """
        >>> ds.writeTable(
        ...     df = df,
        ...     tblName = "lh_silver_vgs.Order",
        ...     format = Format.Delta,
        ...     mode = SaveMode.Overwrite,
        ...     options = {
        ...         "overwriteSchema": "true"
        ...     }
        ... )
        """
        writer: DataFrameWriter = df.write
        
        if format:
            writer = writer.format(format.value)
        if mode:
            writer = writer.mode(mode.value)
        if partitionColsName:
            writer = writer.partitionBy(*partitionColsName)
        if options:
            writer = writer.options(**options)
        
        writer.saveAsTable(tblName) 
    
    def mergeInto(
        self,
        tblName: String
    ) -> MergeIntoWriter:
        return MergeIntoWriter(tblName)
