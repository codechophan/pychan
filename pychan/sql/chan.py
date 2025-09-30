from pyspark.sql.session import SparkSession

from pychan.sql.catalog import Catalog
from pychan.sql.dataset import Dataset

class Chan:
    def __init__(self):
        self._spark: SparkSession = SparkSession.getActiveSession()
    
    @property
    def catalog(self) -> Catalog:
        return Catalog(self._spark)
    
    @property
    def ds(self) -> Dataset:
        return Dataset(self._spark)

chan = Chan()
