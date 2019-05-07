package com.hamshif.wielder.pipelines.snippets

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._

class MaxQualityFastQ extends UserDefinedAggregateFunction {
  // This is the input fields for your aggregate function.
  override def inputSchema: org.apache.spark.sql.types.StructType =
    StructType(StructField("value", DoubleType) :: Nil)

  // This is the internal fields you keep for computing your aggregate.
  override def bufferSchema: StructType = StructType(
      StructField("product", DoubleType) :: Nil
  )

  // This is the output type of your aggregation function.
  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  // This is the initial value for your buffer schema.
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 1.0
  }

  // This is how to update your buffer schema given an input.
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

    val v = Math.max(buffer.getAs[Double](0), input.getAs[Double](0))

    buffer(0) = Math.max(buffer.getAs[Double](0), input.getAs[Double](0))

    val vv = ""
  }

  // This is how to merge two objects with the bufferSchema type.
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

    val v = buffer2.getAs[Double](0)

    buffer1(0) = v
//    buffer1(0) = buffer1.getAs[Long](0) + buffer2.getAs[Long](0)
//    buffer1(1) = buffer1.getAs[Double](1) * buffer2.getAs[Double](1)
  }

  // This is where you output the final value, given the final value of your bufferSchema.
  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(0)
  }
}
