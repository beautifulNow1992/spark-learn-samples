package org.apache.spark.examples


import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.sources._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Row, SQLContext, SparkSession}

import scala.util.Random

/**
  *
  * @param props
  * @param userSchema
  * @param sparkSession
  */
class CustomRelation(props: Map[String, String], userSchema: StructType = null)(@transient val sparkSession: SparkSession)
  extends BaseRelation with TableScan with PrunedScan with PrunedFilteredScan with Serializable {

  @transient val logger = Logger.getLogger(classOf[CustomRelation])

  override def buildScan(): RDD[Row] = {
    buildScan(schema.fieldNames)
  }

  override def schema: StructType = {
    if (this.userSchema != null) userSchema
    else {
      CustomRelation.readSchema(props)
    }

  }

  override def buildScan(requiredColumns: Array[String]): RDD[Row] = {

    val data = (0 to 10000).map(_ => Seq.fill(10)(Random.nextDouble))
    val rdd = sqlContext.sparkContext.parallelize(data)
    rdd.map(s => Row.fromSeq(s))
  }

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {


    buildScan(requiredColumns)
  }


}

object CustomRelation {

  def readSchema(props: Map[String, String]): StructType = {

    //StructType
    StructType.apply((1 to 10).map(index => StructField("test" + index, DataTypes.LongType, nullable = true)))
  }


}