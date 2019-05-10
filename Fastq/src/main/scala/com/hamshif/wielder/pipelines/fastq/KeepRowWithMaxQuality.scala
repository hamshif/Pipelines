    package com.hamshif.wielder.pipelines.fastq

import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._


    class KeepRowWithMaxQuality extends UserDefinedAggregateFunction with FastQKeys {
      // This is the input fields for your aggregate function.
      override def inputSchema: org.apache.spark.sql.types.StructType =
        StructType(
          StructField(KEY_SEQUENCE_IDENTIFIER, StringType) ::
          StructField(KEY_SEQUENCE, StringType) ::
          StructField(KEY_QUALITY_SCORE_IDENTIFIER, StringType) ::
          StructField(KEY_QUALITY_SCORE, StringType) ::

          StructField(KEY_S_SEQUENCE_IDENTIFIER, StringType) ::
          StructField(KEY_S_SEQUENCE, StringType) ::
          StructField(KEY_S_QUALITY_SCORE_IDENTIFIER, StringType) ::
          StructField(KEY_S_QUALITY_SCORE, StringType) ::

          StructField(KEY_ACC_QUALITY_SCORE, LongType) :: Nil
        )

      // This is the internal fields you keep for computing your aggregate.
      override def bufferSchema: StructType =
        StructType(
          StructField(KEY_SEQUENCE_IDENTIFIER, StringType) ::
          StructField(KEY_SEQUENCE, StringType) ::
          StructField(KEY_QUALITY_SCORE_IDENTIFIER, StringType) ::
          StructField(KEY_QUALITY_SCORE, StringType) ::

          StructField(KEY_S_SEQUENCE_IDENTIFIER, StringType) ::
          StructField(KEY_S_SEQUENCE, StringType) ::
          StructField(KEY_S_QUALITY_SCORE_IDENTIFIER, StringType) ::
          StructField(KEY_S_QUALITY_SCORE, StringType) ::

          StructField(KEY_ACC_QUALITY_SCORE, LongType) :: Nil
        )


      // This is the output type of your aggregation function.
      override def dataType: DataType =
        StructType((Array(
          StructField(KEY_SEQUENCE_IDENTIFIER, StringType),
          StructField(KEY_SEQUENCE, StringType),
          StructField(KEY_QUALITY_SCORE_IDENTIFIER, StringType),
          StructField(KEY_QUALITY_SCORE, StringType),

          StructField(KEY_S_SEQUENCE_IDENTIFIER, StringType),
          StructField(KEY_S_SEQUENCE, StringType),
          StructField(KEY_S_QUALITY_SCORE_IDENTIFIER, StringType),
          StructField(KEY_S_QUALITY_SCORE, StringType),

          StructField(KEY_ACC_QUALITY_SCORE, LongType)
        )))

      override def deterministic: Boolean = true

      // This is the initial value for your buffer schema.
      override def initialize(buffer: MutableAggregationBuffer): Unit = {
        buffer(0) = ""
        buffer(1) = ""
        buffer(2) = ""
        buffer(3) = ""
        buffer(4) = ""
        buffer(5) = ""
        buffer(6) = ""
        buffer(7) = ""
        buffer(8) = 0L
      }

      // This is how to update your buffer schema given an input.
      override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {

        val amt = buffer.getAs[Long](8)
        val candidateAmt = input.getAs[Long](8)

        amt match {
          case a if a < candidateAmt =>
            buffer(0) = input.getAs[String](0)
            buffer(1) = input.getAs[String](1)
            buffer(2) = input.getAs[String](2)
            buffer(3) = input.getAs[String](3)
            buffer(4) = input.getAs[String](4)
            buffer(5) = input.getAs[String](5)
            buffer(6) = input.getAs[String](6)
            buffer(7) = input.getAs[String](7)
            buffer(8) = input.getAs[Long](8)
          case _ =>
        }
      }

      // This is how to merge two objects with the bufferSchema type.
      override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {

        buffer1(0) = buffer2.getAs[String](0)
        buffer1(1) = buffer2.getAs[String](1)
        buffer1(2) = buffer2.getAs[String](2)
        buffer1(3) = buffer2.getAs[String](3)
        buffer1(4) = buffer2.getAs[String](4)
        buffer1(5) = buffer2.getAs[String](5)
        buffer1(6) = buffer2.getAs[String](6)
        buffer1(7) = buffer2.getAs[String](7)
        buffer1(8) = buffer2.getAs[Long](8)
      }

      // This is where you output the final value, given the final value of your bufferSchema.
      override def evaluate(buffer: Row): Any = {
        buffer
      }
    }
