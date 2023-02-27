import pyspark.sql.functions as F
from awsglue import DynamicFrame
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType
from pyspark.context import SparkContext
from pyspark.sql.functions import col,isnan, when, count, regexp_replace

def fill_empty_null_values_txn(
    self, columnName, newValue
):      
        gluectx = self.glue_ctx       
        _df = self.toDF()
        if _df.filter(col(columnName)=="").count() > 0:
            modifiedDF = _df.withColumn(columnName,when(col(columnName)=="" , newValue).otherwise(col(columnName)))
            _dyf = DynamicFrame.fromDF(modifiedDF, self.glue_ctx, self.name)
            return _dyf
        elif _df.filter(col(columnName).isNull()).count() > 0:  
            _df = _df.fillna(value=newValue, subset=[columnName])
            _dyf = DynamicFrame.fromDF(_df, self.glue_ctx, self.name)
            return _dyf
        return self

DynamicFrame.fill_empty_null_values_txn = fill_empty_null_values_txn