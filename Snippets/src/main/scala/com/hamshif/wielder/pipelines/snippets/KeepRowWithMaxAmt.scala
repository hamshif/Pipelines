    package com.hamshif.wielder.pipelines.snippets

    import org.apache.spark.sql.Row
    import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
    import org.apache.spark.sql.types._


    class KeepRowWithMaxAmt extends UserDefinedAggregateFunction {
      // This is the input fields for your aggregate function.
      override def inputSchema: org.apache.spark.sql.types.StructType =
        StructType(
          StructField("store", StringType) ::
          StructField("prod", StringType) ::
          StructField("amt", DoubleType) ::
          StructField("units", IntegerType) :: Nil
        )

      // This is the internal fields you keep for computing your aggregate.
      override def bufferSchema: StructType = StructType(
        StructField("store", StringType) ::
        StructField("prod", StringType) ::
        StructField("amt", DoubleType) ::
        StructField("units", IntegerType) :: Nil
      )


      // This is the output type of your aggregation function.
      override def dataType: DataType =
        StructType((Array(
          StructField("store", StringType),
          StructField("prod", StringType),
          StructField("amt", DoubleType),
          StructField("units", IntegerType)
        )))

      override def deterministic: Boolean = true

      // This is the initial value for your buffer schema.
      override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = ""
        buffer(1) = ""
        buffer(2) = 0.0
        buffer(3) = 0
      }

      // This is how to update your buffer schema given an input.
      override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

        val amt = buffer.getAs[Double](2)
        val candidateAmt = input.getAs[Double](2)

        amt match {
          case a if a < candidateAmt =>
            buffer(0) = input.getAs[String](0)
            buffer(1) = input.getAs[String](1)
            buffer(2) = input.getAs[Double](2)
            buffer(3) = input.getAs[Int](3)
          case _ =>
        }
      }

      // This is how to merge two objects with the bufferSchema type.
      override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

        buffer1(0) = buffer2.getAs[String](0)
        buffer1(1) = buffer2.getAs[String](1)
        buffer1(2) = buffer2.getAs[Double](2)
        buffer1(3) = buffer2.getAs[Int](3)
      }

      // This is where you output the final value, given the final value of your bufferSchema.
      override def evaluate(buffer: Row): Any = {
        buffer
      }
    }
