package org.dmlaran

import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row

class RangoUADF extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(StructField("valor", DoubleType) :: Nil)
  override def bufferSchema: StructType = StructType(
    StructField("minVal", DoubleType) ::
    StructField("maxVal", DoubleType) :: Nil
  )
  override def dataType: DataType = DoubleType
  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Double.PositiveInfinity
    buffer(1) = Double.NegativeInfinity
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      val valor = input.getDouble(0)
      buffer(0) = math.min(buffer.getDouble(0), valor)
      buffer(1) = math.max(buffer.getDouble(1), valor)
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = math.min(buffer1.getDouble(0), buffer2.getDouble(0))
    buffer1(1) = math.max(buffer1.getDouble(1), buffer2.getDouble(1))
  }

  override def evaluate(buffer: Row): Any = {
    buffer.getDouble(1) - buffer.getDouble(0)
  }
}
