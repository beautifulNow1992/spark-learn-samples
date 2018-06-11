package org.apache.spark.examples.datasource

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}


object CustomRelationTest extends App {

  val structType = StructType.apply((1 to 10).map(index => StructField("test" + index, DataTypes.LongType, nullable = true)))
  println(structType)

}
