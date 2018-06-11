package org.apache.spark.examples.ml.lib

import java.util.UUID

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructType}
import org.apache.spark.sql.{DataFrame, Dataset}

class UUIDTransformer(override val uid: String = Identifiable.randomUID("identity")) extends Transformer {
  override def transform(inputData: Dataset[_]): DataFrame = {
    inputData
      .withColumn("_UUID", lit(UUID.randomUUID().getMostSignificantBits));
  }

  override def copy(paramMap: ParamMap) = this

  override def transformSchema(schema: StructType) = schema.add("_UUID", DataTypes.StringType, false)
}
