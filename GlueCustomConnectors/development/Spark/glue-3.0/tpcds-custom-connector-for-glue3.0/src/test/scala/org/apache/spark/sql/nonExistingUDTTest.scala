package org.apache.spark.sql

import org.apache.spark.sql.types.{DataType, SQLUserDefinedType, StringType, UserDefinedType}

@SQLUserDefinedType(udt = classOf[nonExistingUDTTest])
class nonExistingTest extends Serializable

class nonExistingUDTTest extends UserDefinedType[nonExistingTest] {
  override def sqlType: DataType = StringType
  override def userClass: Class[nonExistingTest] = classOf[nonExistingTest]

  override def asNullable: nonExistingUDTTest = this

  override def serialize(obj: nonExistingTest): Any = obj.toString
  override def deserialize(datum: Any): nonExistingTest = new nonExistingTest
}