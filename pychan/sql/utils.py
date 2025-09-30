from pychan.types import (
    String,
    List,
    Set,
    Optional
)

class Utils:
    def __init__(self):
        self._merge = MergeUtils()
    
    @property
    def merge(self) -> "MergeUtils":
        return self._merge

class MergeUtils:
    def _genIdCondition(
        self,
        idColsName: List[String]
    ) -> String:
        """
        >>> idColsName: List[String] = ["BELNR", "DOCLN", "RCLNT", "RLDNR", "RBUKRS", "GJAHR"]
        >>> chanutils.merge._genIdCondition(idColsName)
        "target.BELNR = source.BELNR AND target.DOCLN = source.DOCLN AND target.RCLNT = source.RCLNT AND target.RLDNR = source.RLDNR AND target.RBUKRS = source.RBUKRS AND target.GJAHR = source.GJAHR"
        """
        return (
            " AND "
                .join([
                    f"target.{idColName} = source.{idColName}"
                    for idColName in idColsName
                ])
        )
    
    def _genPartitionCondition(
        self,
        partitionColsName: Optional[List[String]],
        partitionValues: Optional[List[String]]
    ) -> String:
        """
        >>> partitionColsName: List[String] = ["RBUKRS", "GJAHR"]
        >>> partitionValues: List[String] = ["'S030'", "'2025', '2024'"]
        >>> chanutils.merge._genPartitionCondition(partitionColsName, partitionValues)
        "target.RBUKRS IN ('S030') AND target.GJAHR IN ('2025', '2024')"
        """
        return (
            " AND "
                .join([
                    f"target.{partitionColName} IN ({partitionValue})"
                    for (partitionColName, partitionValue) in zip(partitionColsName, partitionValues)
                ])
        )
    
    def genCondition(
        self,
        idColsName: List[String],
        partitionColsName: Optional[List[String]] = None,
        partitionValues: Optional[List[String]] = None
    ) -> String:
        """
        >>> idColsName: List[String] = ["BELNR", "DOCLN", "RCLNT", "RLDNR", "RBUKRS", "GJAHR"]
        >>> partitionColsName: List[String] = ["RBUKRS", "GJAHR"]
        >>> partitionValues: List[String] = ["'S030'", "'2025', '2024'"]
        >>> condition: String = chanutils.merge.genCondition(
        ...     idColsName,
        ...     partitionColsName,
        ...     partitionValues
        ... )
        >>> condition
        "target.BELNR = source.BELNR AND target.DOCLN = source.DOCLN AND target.RCLNT = source.RCLNT AND target.RLDNR = source.RLDNR AND target.RBUKRS = source.RBUKRS AND target.GJAHR = source.GJAHR AND target.RBUKRS IN ('S030') AND target.GJAHR IN ('2025', '2024')"
        
        >>> condition: String = chanutils.merge.genCondition(idColsName)
        >>> condition
        "target.BELNR = source.BELNR AND target.DOCLN = source.DOCLN AND target.RCLNT = source.RCLNT AND target.RLDNR = source.RLDNR AND target.RBUKRS = source.RBUKRS AND target.GJAHR = source.GJAHR"
        """
        idCondition: String = self._genIdCondition(idColsName)
        
        if (
            partitionColsName
            and partitionValues
        ):
            partitionCondition: String = self._genPartitionCondition(partitionColsName, partitionValues)
            return f"{idCondition} AND {partitionCondition}"
        
        return idCondition
    
    def genSet(
        self,
        colsName: List[String],
        ignoreColsName: Optional[Set[String] | List[String]] = None
    ) -> String:
        """
        >>> colsName: List["String"] = ["BELNR", "DOCLN", "RCLNT", "RLDNR", "RBUKRS", "GJAHR", "__created_timestamp"]
        >>> ignoreColsName: Set[String] = {"__created_timestamp"}
        >>> chanutils.merge.genSet(colsName, ignoreColsName)
        "BELNR = source.BELNR, DOCLN = source.DOCLN, RCLNT = source.RCLNT, RLDNR = source.RLDNR, RBUKRS = source.RBUKRS, GJAHR = source.GJAHR"
        """
        ignoreColsName = ignoreColsName or set()
        
        return (
            ", "
                .join([
                    f"{colName} = source.{colName}"
                    for colName in colsName
                    if colName not in ignoreColsName
                ])
        )
    
    def genValues(self, colsName: List[String]) -> String:
        """
        >>> colsName: List[String] = ["BELNR", "DOCLN", "RCLNT", "RLDNR", "RBUKRS", "GJAHR"]
        >>> chanutils.merge.genValues(colsName)
        "(BELNR, DOCLN, RCLNT, RLDNR, RBUKRS, GJAHR, __created_timestamp) VALUES (source.BELNR, source.DOCLN, source.RCLNT, source.RLDNR, source.RBUKRS, source.GJAHR, source.__created_timestamp)"
        """
        return (
            f"({', '.join(colsName)}) "
            "VALUES "
            f"({', '.join([f'source.{colName}' for colName in colsName])})"
        )

chanutils = Utils()
