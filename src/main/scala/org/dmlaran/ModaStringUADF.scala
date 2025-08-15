package org.dmlaran

import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import scala.collection.mutable

class ModaStringUADF extends UserDefinedAggregateFunction {

  override def inputSchema: StructType =
    StructType(StructField("valor", StringType) :: Nil)

  override def bufferSchema: StructType =
    StructType(StructField("conteos", MapType(StringType, LongType)) :: Nil)

  override def dataType: DataType = StringType
  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map.empty[String, Long]
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      val valor = input.getString(0)
      val mapa = mutable.Map(buffer.getMap[String, Long](0).toSeq: _*)
      mapa.put(valor, mapa.getOrElse(valor, 0L) + 1L)
      buffer(0) = mapa.toMap
    }
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val mapa1 = mutable.Map(buffer1.getMap[String, Long](0).toSeq: _*)
    val mapa2 = mutable.Map(buffer2.getMap[String, Long](0).toSeq: _*)

    for ((k, v) <- mapa2) {
      mapa1.put(k, mapa1.getOrElse(k, 0L) + v)
    }

    buffer1(0) = mapa1.toMap
  }

  override def evaluate(buffer: Row): Any = {
    val mapa = buffer.getMap[String, Long](0)
    if (mapa.isEmpty) null
    else mapa.maxBy(_._2)._1
  }
}
