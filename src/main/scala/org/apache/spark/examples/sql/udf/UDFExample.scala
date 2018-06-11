package org.apache.spark.examples.sql.udf

object UDFExample extends App {

  val spark = SparkSession
    .builder()
    .appName("Spark SQL UDF example")
    .master("local")
    .getOrCreate()

  spark.udf.register("logic", (x: Double) => 1 / (Math.exp(-x) + 1))

  spark.udf.register("logicUDAF", new UserDefinedAggregateFunction {
    // Define the UDAF input and result schema's
    def inputSchema: StructType = // Input  = (Double price, Long quantity)
      new StructType().add("x", DoubleType)

    def bufferSchema: StructType = // Output = (Double total)
      new StructType().add("y", DoubleType)

    def dataType: DataType = DoubleType

    def deterministic: Boolean = true // true: our UDAF's output given an input is deterministic

    def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer.update(0, 0.0) // Initialize the result to 0.0
    }

    def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
      val y = buffer.getDouble(0) // y
      val x = input.getDouble(0) // x
      buffer.update(0, 1 / (Math.exp(-x) + 1)) // 更新
    }

    //sum汇总 概率分布肯定为1
    def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
      buffer1.update(0, buffer1.getDouble(0) + buffer2.getDouble(0))
    }

    //结果
    def evaluate(buffer: Row): Any = {
      buffer.getDouble(0)
    }
  });

  val dataset = spark.range(1, 1000, 2).toDF()
  dataset.printSchema()
  dataset.show(10)
  dataset.createOrReplaceTempView("test")
  //最后聚合概率分布式结果肯定是1
  spark.sql("select logicUDAF(id) as total from test").show(10)
  //求logic结果
  spark.sql("select logic(id) from test").show(10)
  spark.stop()

}
