from pyspark.sql.session import SparkSession
from pyspark.sql.catalog import Table

from pychan.types import (
    String,
    List,
    Unit
)

class Catalog:
    def __init__(
        self,
        spark: SparkSession
    ) -> Unit:
        self._spark: SparkSession = spark
    
    def getTablesName(
        self,
        tables: List[Table]
    ) -> List[String]:
        """
        >>> tblsName: List[String] = catalog.getTablesName(tbls)
        >>> tblsName
        ['Ticket', 'TicketUsage', 'Sale', 'SaleItem', 'SaleItemDetail', 'Product']
        """
        return [
            tbl.name
            for tbl in tables
            if tbl.tableType == 'MANAGED'
        ]
    
    def getViewsName(
        self,
        tables: List[Table]
    ) -> List[String]:
        """
        >>> viewsName: List[String] = catalog.getViewsName(tbls)
        >>> viewsName
        ['V_TicketUsageInZone']
        """
        return [
            tbl.name
            for tbl in tables
            if tbl.tableType == 'VIEW'
        ]
