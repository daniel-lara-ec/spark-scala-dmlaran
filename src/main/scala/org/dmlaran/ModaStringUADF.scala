package org.dmlaran

import org.apache.spark.sql.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import scala.collection.mutable

class ModaStringUADF extends UserDefinedAggregateFunction {

  // Una columna de entrada tipo String
  override def inputSchema: StructType =
    StructType(StructField("valor", StringType) :: Nil)

  // Buffer: un mapa con String -> Long (conteos)
  override def bufferSchema: StructType =
    StructType(StructField("conteos", MapType(StringType, LongType)) :: Nil)

  override def dataType: DataType = StringType
  override def deterministic: Boolean = true

  // Inicializar buffer vacío
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = Map.empty[String, Long]
  }

  // Actualizar buffer con un nuevo valor
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if (!input.isNullAt(0)) {
      val valor = input.getString(0)
      val mapa = mutable.Map(buffer.getMap .toSeq: _*)
      mapa.put(valor, mapa.getOrElse(valor, 0L) + 1L)
      buffer(0) = mapa.toMap
    }
  }

  // Combinar buffers de distintas particiones
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    val mapa1 = mutable.Map(buffer1.getMap .toSeq: _*)
    val mapa2 = buffer2.getMap 

    for ((k, v) <- mapa2) {
      mapa1.put(k, mapa1.getOrElse(k, 0L) + v)
    }

    buffer1(0) = mapa1.toMap
  }

  // Calcular moda (valor con mayor frecuencia)
  override def evaluate(buffer: Row): Any = {
    val mapa = buffer.getMap 
    if (mapa.isEmpty) null
    else mapa.maxBy(_._2)._1
  }
}
