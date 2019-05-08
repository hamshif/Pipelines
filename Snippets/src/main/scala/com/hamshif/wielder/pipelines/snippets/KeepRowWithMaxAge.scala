package com.hamshif.wielder.pipelines.snippets

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


  class KeepRowWithMaxAge extends UserDefinedAggregateFunction {
    // This is the input fields for your aggregate function.
    override def inputSchema: org.apache.spark.sql.types.StructType =
      StructType(
        StructField("id", LongType) ::
        StructField("name", StringType) ::
        StructField("requisite", StringType) ::
        StructField("money", DoubleType) ::
        StructField("age", IntegerType) :: Nil
      )

    // This is the internal fields you keep for computing your aggregate.
    override def bufferSchema: StructType = StructType(
      StructField("id", LongType) ::
      StructField("name", StringType) ::
      StructField("requisite", StringType) ::
      StructField("money", DoubleType) ::
      StructField("age", IntegerType) :: Nil
    )


    // This is the output type of your aggregation function.
    override def dataType: DataType =
      StructType((Array(
        StructField("id", LongType),
        StructField("name", StringType),
        StructField("requisite", StringType),
        StructField("money", DoubleType),
        StructField("age", IntegerType)
      )))

    override def deterministic: Boolean = true

    // This is the initial value for your buffer schema.
    override def initialize(buffer: MutableAggregationBuffer): Unit = {
      buffer(0) = 0L
      buffer(1) = "empty"
      buffer(2) = "empty"
      buffer(3) = 0.0
      buffer(4) = 0
    }

    // This is how to update your buffer schema given an input.
    override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

      val age = buffer.getAs[Int](4)
      val candidateAge = input.getAs[Int](4)

      println(s"class ${age.getClass}  age $age")
      println(s"class ${candidateAge.getClass}  candidateAge $candidateAge")

      age match {
        case a if a < candidateAge =>
          buffer(0) = input.getAs[Long](0)
          buffer(1) = input.getAs[String](1)
          buffer(2) = input.getAs[String](2)
          buffer(3) = input.getAs[Double](3)
          buffer(4) = input.getAs[Int](4)
        case _ =>
      }

    }

    // This is how to merge two objects with the bufferSchema type.
    override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

      buffer1(0) = buffer2.getAs[Long](0)
      buffer1(1) = buffer2.getAs[String](1)
      buffer1(2) = buffer2.getAs[String](2)
      buffer1(3) = buffer2.getAs[Double](3)
      buffer1(4) = buffer2.getAs[Int](4)
    }

    // This is where you output the final value, given the final value of your bufferSchema.
    override def evaluate(buffer: Row): Any = {
      buffer
    }
  }
