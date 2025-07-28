package org.dmlaran

import org.apache.spark.sql.api.java.UDF1
import org.apache.spark.sql.api.java.UDF3
import org.apache.spark.sql.api.java.UDF4
import scala.util.matching.Regex
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

class ProyeccionLatitudY extends UDF1[Object, java.lang.Double] {
  override def call(lat: Object): java.lang.Double = {
    val latDouble: Double = lat match {
      case i: Integer          => i.toDouble
      case l: java.lang.Long   => l.toDouble
      case f: java.lang.Float  => f.toDouble
      case d: java.lang.Double => d
      case _ =>
        throw new IllegalArgumentException("Tipo no soportado: " + lat.getClass)
    }

    val latRad = toRadians(latDouble)
    val R = 6378137.0
    val y = R * log(tan(Pi / 4 + latRad / 2))
    y / 1000.0
  }
}

class ProyeccionLongitudX extends UDF1[Object, java.lang.Double] {
  override def call(lon: Object): java.lang.Double = {
    val lonDouble: Double = lon match {
      case i: Integer          => i.toDouble
      case l: java.lang.Long   => l.toDouble
      case f: java.lang.Float  => f.toDouble
      case d: java.lang.Double => d
      case _ =>
        throw new IllegalArgumentException("Tipo no soportado: " + lon.getClass)
    }

    val lonRad = toRadians(lonDouble)
    val R = 6378137.0
    val x = R * lonRad
    x / 1000.0
  }
}


class RayoIntersecaArista extends UDF3[Any, Any, String, java.lang.Boolean] {

  case class Punto(lat: Double, lon: Double)

  private def parsearArista(aristaStr: String): Option[(Punto, Punto)] = {
    val regex = """\(\((-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)\),\s*\((-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)\)\)""".r
    aristaStr match {
      case regex(lat1, lon1, lat2, lon2) =>
        Some(
          (
            Punto(lat1.toDouble, lon1.toDouble),
            Punto(lat2.toDouble, lon2.toDouble)
          )
        )
      case _ => None
    }
  }

  override def call(lat: Any, lon: Any, aristaStr: String): java.lang.Boolean = {
    // Conversión segura de tipos
    val latDouble: Double = lat match {
      case i: Integer          => i.toDouble
      case l: java.lang.Long   => l.toDouble
      case f: java.lang.Float  => f.toDouble
      case d: java.lang.Double => d
      case _ =>
        throw new IllegalArgumentException("Latitud no soportada: " + lat.getClass)
    }

    val lonDouble: Double = lon match {
      case i: Integer          => i.toDouble
      case l: java.lang.Long   => l.toDouble
      case f: java.lang.Float  => f.toDouble
      case d: java.lang.Double => d
      case _ =>
        throw new IllegalArgumentException("Longitud no soportada: " + lon.getClass)
    }

    parsearArista(aristaStr) match {
      case Some((p1, p2)) =>
        val x = lonDouble
        val y = latDouble

        val (x1, y1) = (p1.lon, p1.lat)
        val (x2, y2) = (p2.lon, p2.lat)

        val (xa, ya, xb, yb) =
          if (y1 <= y2) (x1, y1, x2, y2)
          else (x2, y2, x1, y1)

        if (y <= ya || y > yb) {
          false
        } else {
          val xIntersect = xa + (xb - xa) * (y - ya) / (yb - ya)
          xIntersect > x
        }

      case None =>
        throw new IllegalArgumentException(s"Formato inválido de arista: $aristaStr")
    }
  }
}


class RayoIntersecaAristaRectangular extends UDF3[Any, Any, String, java.lang.Boolean] {

  case class Punto(lat: Double, lon: Double)

  private def parsearArista(aristaStr: String): Option[(Punto, Punto)] = {
    val regex = """\(\((-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)\),\s*\((-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)\)\)""".r
    aristaStr match {
      case regex(lat1, lon1, lat2, lon2) =>
        Some(
          (
            Punto(lat1.toDouble, lon1.toDouble),
            Punto(lat2.toDouble, lon2.toDouble)
          )
        )
      case _ => None
    }
  }

  private def proyectarMercator(lat: Double, lon: Double): (Double, Double) = {
    val R = 6378137.0
    val x = R * toRadians(lon)
    val y = R * log(tan(Pi / 4 + toRadians(lat) / 2))
    (x, y)
  }

  override def call(lat: Any, lon: Any, aristaStr: String): java.lang.Boolean = {
    // Conversión segura de tipos
    val latDouble: Double = lat match {
      case i: Integer          => i.toDouble
      case l: java.lang.Long   => l.toDouble
      case f: java.lang.Float  => f.toDouble
      case d: java.lang.Double => d
      case _ =>
        throw new IllegalArgumentException("Latitud no soportada: " + lat.getClass)
    }

    val lonDouble: Double = lon match {
      case i: Integer          => i.toDouble
      case l: java.lang.Long   => l.toDouble
      case f: java.lang.Float  => f.toDouble
      case d: java.lang.Double => d
      case _ =>
        throw new IllegalArgumentException("Longitud no soportada: " + lon.getClass)
    }

    parsearArista(aristaStr) match {
      case Some((p1, p2)) =>
        // Proyectar punto
        val (x, y) = proyectarMercator(latDouble, lonDouble)

        // Proyectar extremos de la arista
        val (x1, y1) = proyectarMercator(p1.lat, p1.lon)
        val (x2, y2) = proyectarMercator(p2.lat, p2.lon)

        // Ordenar verticalmente
        val (xa, ya, xb, yb) =
          if (y1 <= y2) (x1, y1, x2, y2)
          else (x2, y2, x1, y1)

        if (y <= ya || y > yb) {
          false
        } else {
          val xIntersect = xa + (xb - xa) * (y - ya) / (yb - ya)
          xIntersect > x
        }

      case None =>
        throw new IllegalArgumentException(s"Formato inválido de arista: $aristaStr")
    }
  }
}

class ProyectarAristaMercator extends UDF1[String, String] {

  private val regex = """\(\((-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)\),\s*\((-?\d+(?:\.\d+)?),\s*(-?\d+(?:\.\d+)?)\)\)""".r

  private def proyectar(lat: Double, lon: Double): (Double, Double) = {
    val R = 6378137.0
    val x = (R * toRadians(lon)) / 1000.0 // Convertir a kilómetros
    val y = (R * log(tan(Pi / 4 + toRadians(lat) / 2))) / 1000.0 // Convertir a kilómetros
    (x, y)
  }

  override def call(aristaStr: String): String = {
    aristaStr match {
      case regex(lat1, lon1, lat2, lon2) =>
        val (x1, y1) = proyectar(lat1.toDouble, lon1.toDouble)
        val (x2, y2) = proyectar(lat2.toDouble, lon2.toDouble)
        // Devuelvo string con formato original (lat, lon) pero proyectados (y es lat, x es lon en Mercator)
        f"(($y1%.6f,$x1%.6f),($y2%.6f,$x2%.6f))"
      case _ =>
        throw new IllegalArgumentException(s"Formato inválido de arista: $aristaStr")
    }
  }
}
