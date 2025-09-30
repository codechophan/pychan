from pychan.types import (
    String,
    List,
    Optional,
    Unit
)

class MergeIntoWriter:
    def __init__(
        self,
        tblName: String
    ) -> Unit:
        self._targetAs: String = "target"
        self._sourceAs: String = "source"
        self._tblName: String = tblName
        self._viewName: Optional[String] = None
        self._condition: Optional[String] = None
        self._matchedActions: List[String] = []
        self._notMatchedActions: List[String] = []
        self._notMatchedBySourceActions: List[String] = []
    
    def using(
        self,
        viewName: String
    ) -> "MergeIntoWriter":
        self._viewName = viewName
        return self
    
    def on(
        self,
        condition: String
    ) -> "MergeIntoWriter":
        self._condition = condition
        return self
    
    def _clearActions(self) -> "MergeIntoWriter":
        self._matchedActions.clear()
        self._notMatchedActions.clear()
        self._notMatchedBySourceActions.clear()
        return self
    
    def generate(self) -> String:
        """
        >>> tblName = "lh.Sales"
        >>> viewName = "{df}"
        >>> condition = "target.id = source.id"
        >>> print(
        ...     chan
        ...         .ds
        ...         .mergeInto(tblName)
        ...         .using(viewName)
        ...         .on(condition)
        ...         .whenMatched()
        ...         .updateAll()
        ...         .whenNotMatched()
        ...         .insertAll()
        ...         .generate()
        ... )
        MERGE INTO lh.Sales AS target
        USING {df} AS source
        ON target.id = source.id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
        """
        if not all([
            self._tblName,
            self._viewName,
            self._condition
        ]):
            raise ValueError(f"{__name__}.generate(): Missing required parameters.")
        
        if not any([
            self._matchedActions,
            self._notMatchedActions,
            self._notMatchedBySourceActions
        ]):
            raise ValueError(f"{__name__}.generate(): No merge actions provided.")
        
        sql: List[String] = [
            f"MERGE INTO {self._tblName} AS {self._targetAs}",
            f"USING {self._viewName} AS {self._sourceAs}",
            f"ON {self._condition}",
            *self._matchedActions,
            *self._notMatchedActions,
            *self._notMatchedBySourceActions
        ]
        
        self._clearActions()
        return "\n".join(sql)
    
    def whenMatched(
        self,
        condition: Optional[String] = None
    ) -> "MergeIntoWriter.WhenMatched":
        return self.WhenMatched(self, condition)
    
    def whenNotMatched(
        self,
        condition: Optional[String] = None
    ) -> "MergeIntoWriter.WhenNotMatched":
        return self.WhenNotMatched(self, condition)
    
    def whenNotMatchedBySource(
        self,
        condition: Optional[String] = None
    ) -> "MergeIntoWriter.WhenNotMatchedBySource":
        return self.WhenNotMatchedBySource(self, condition)
    
    class WhenMatched:
        def __init__(
            self,
            writer: "MergeIntoWriter",
            condition: Optional[String]
        ) -> Unit:
            self.writer = writer
            self.condition = condition
        
        def _genWhen(self) -> String:
            return f"WHEN MATCHED AND {self.condition}" if self.condition else "WHEN MATCHED"
        
        def updateAll(self) -> "MergeIntoWriter":
            self.writer._matchedActions.append(f"{self._genWhen()} THEN UPDATE SET *")
            return self.writer
        
        def updateExpr(
            self,
            set: String
        ) -> "MergeIntoWriter":
            self.writer._matchedActions.append(f"{self._genWhen()} THEN UPDATE SET {set}")
            return self.writer
        
        def delete(self) -> "MergeIntoWriter":
            self.writer._matchedActions.append(f"{self._genWhen()} THEN DELETE")
            return self.writer
    
    class WhenNotMatched:
        def __init__(
            self,
            writer: "MergeIntoWriter",
            condition: Optional[String]
        ) -> Unit:
            self.writer = writer
            self.condition = condition
        
        def _genWhen(self) -> String:
            return f"WHEN NOT MATCHED AND {self.condition}" if self.condition else "WHEN NOT MATCHED"
        
        def insertAll(self) -> "MergeIntoWriter":
            self.writer._notMatchedActions.append(f"{self._genWhen()} THEN INSERT *")
            return self.writer
        
        def insertExpr(
            self,
            values: String
        ) -> "MergeIntoWriter":
            self.writer._notMatchedActions.append(f"{self._genWhen()} THEN INSERT {values}")
            return self.writer
    
    class WhenNotMatchedBySource:
        def __init__(
            self,
            writer: "MergeIntoWriter",
            condition: Optional[String]
        ) -> Unit:
            self.writer = writer
            self.condition = condition
        
        def _genWhen(self) -> String:
            return f"WHEN NOT MATCHED BY SOURCE AND {self.condition}" if self.condition else "WHEN NOT MATCHED BY SOURCE"
        
        def updateAll(self) -> "MergeIntoWriter":
            self.writer._notMatchedBySourceActions.append(f"{self._genWhen()} THEN UPDATE SET *")
            return self.writer
        
        def updateExpr(
            self,
            set: String
        ) -> "MergeIntoWriter":
            self.writer._notMatchedBySourceActions.append(f"{self._genWhen()} THEN UPDATE SET {set}")
            return self.writer
        
        def delete(self) -> "MergeIntoWriter":
            self.writer._notMatchedBySourceActions.append(f"{self._genWhen()} THEN DELETE")
            return self.writer
