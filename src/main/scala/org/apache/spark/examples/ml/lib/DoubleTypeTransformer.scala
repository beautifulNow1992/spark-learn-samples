package org.apache.spark.examples.ml.lib

import org.apache.spark.ml.UnaryTransformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.types.{DataType, DataTypes}

class DoubleTypeTransformer(override val uid: String) extends UnaryTransformer[Seq[String], Seq[Double], DoubleTypeTransformer] {

  def this() = this(Identifiable.randomUID("DoubleTypeTransformer"))

  override protected def createTransformFunc: (Seq[String]) => Seq[Double] = {
    _.iterator.map(DoubleTypeTransformer.toDouble).toSeq
  }

  override protected def outputDataType: DataType = DataTypes.DoubleType
}

object DoubleTypeTransformer {


  private def toDouble(str: String): Double = {
    str.toDouble
  }
}
