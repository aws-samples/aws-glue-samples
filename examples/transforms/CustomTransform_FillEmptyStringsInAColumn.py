import pyspark.sql.functions as F
from awsglue import DynamicFrame
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import StructType
from pyspark.context import SparkContext
from pyspark.sql.functions import col,isnan, when, count, regexp_replace
from typing import Union

def get_col_type(df, colName):
    if type(df) == DynamicFrame:
        df = df.toDF()
    return df.select(colName).dtypes[0][1]

def has_choices(dynf):
    def inner(fields):
        from awsglue.gluetypes import ChoiceType, StructType
        for field in fields:
            field_type = field.dataType.__class__.__name__
            if field_type == ChoiceType.__name__:
                return True
            elif field_type == StructType.__name__:
                if inner(field.dataType.fields):
                    return True
        return False
    return inner(dynf.schema())

def enrich_df(name, function):
    def transform(self, **kwargs) -> Union[DynamicFrame, DataFrame]:
        if type(self) == DynamicFrame:
            if has_choices(self):
                # Converting to DataFrame and back with choices is not symmetric, choices become structs, prevent this
                raise ValueError("Please resolve the pending DynamicFrame choices before using this transformation")
            # Convert, apply and convert back
            return DynamicFrame.fromDF(function(self.toDF(), **kwargs), self.glue_ctx, self.name)
        else:
            return function(self, **kwargs)

def fill_empty_string_values_txn(
    self, columnName, newValue
) -> Union[DynamicFrame, DataFrame]:
    if type(self) == DynamicFrame:
        gluectx = self.glue_ctx
        _df = self.toDF()
        modifiedDF = _df.withColumn(columnName,when(col(columnName)=="" , newValue).otherwise(col(columnName)))
        _dyf = DynamicFrame.fromDF(modifiedDF, self.glue_ctx, self.name)
        return _dyf
    elif type(self) == DataFrame:
        _df = self; 
        modifiedDF = _df.withColumn(columnName,when(col(columnName)=="" , newValue).otherwise(col(columnName)))
        return modifiedDF

# Register function with DataFrame and DynamicFrame classes on import.
# Must be imported before DataFrame or DynamicFrame is instantiated
DataFrame.fill_empty_string_values_txn = fill_empty_string_values_txn
DynamicFrame.fill_empty_string_values_txn = fill_empty_string_values_txn

enrich_df('FillEmptyStringColumns', fill_empty_string_values_txn)
