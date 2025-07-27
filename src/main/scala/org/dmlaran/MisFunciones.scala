package org.dmlaran

import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.api.java.UDF4
import scala.math._

class CuadradoUDF extends UDF1[Any, java.lang.Double] {
  override def call(t: Any): java.lang.Double = t match {
    case i: java.lang.Integer => i.toDouble * i.toDouble
    case l: java.lang.Long    => l.toDouble * l.toDouble
    case f: java.lang.Float   => f.toDouble * f.toDouble
    case d: java.lang.Double  => d * d
    case _                    => Double.NaN
  }
}

class DistanciaHaversine
    extends UDF4[Object, Object, Object, Object, java.lang.Double] {
  override def call(
      lat1: Object,
      lon1: Object,
      lat2: Object,
      lon2: Object
  ): java.lang.Double = {
    if (lat1 == null || lon1 == null || lat2 == null || lon2 == null)
      return null

    def toDouble(v: Object): Double = v match {
      case i: Integer          => i.toDouble
      case l: java.lang.Long   => l.toDouble
      case f: java.lang.Float  => f.toDouble
      case d: java.lang.Double => d
      case _ =>
        throw new IllegalArgumentException("Tipo no soportado: " + v.getClass)
    }

    val lat1d = Math.toRadians(toDouble(lat1))
    val lon1d = Math.toRadians(toDouble(lon1))
    val lat2d = Math.toRadians(toDouble(lat2))
    val lon2d = Math.toRadians(toDouble(lon2))

    val dlat = lat2d - lat1d
    val dlon = lon2d - lon1d
    val a = Math.pow(Math.sin(dlat / 2), 2) +
      Math.cos(lat1d) * Math.cos(lat2d) * Math.pow(Math.sin(dlon / 2), 2)
    val c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a))

    val earthRadiusKm = 6371.0
    earthRadiusKm * c
  }
}
