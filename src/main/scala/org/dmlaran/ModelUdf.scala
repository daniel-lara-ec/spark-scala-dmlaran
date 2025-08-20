package org.dmlaran

import org.apache.spark.sql.api.java.UDF1
import ai.onnxruntime._
import org.apache.spark.SparkFiles
import scala.collection.JavaConverters._
import java.util

object ONNXSession {
  lazy val env: OrtEnvironment = OrtEnvironment.getEnvironment

  lazy val session: OrtSession = {
    val modelPath = SparkFiles.get("encoderV2.onnx")
    val opts = new OrtSession.SessionOptions()
    opts.setIntraOpNumThreads(1)
    opts.setInterOpNumThreads(1)
    env.createSession(modelPath, opts)
  }
}

class AutoencoderUDF
    extends UDF1[Seq[java.lang.Double], Seq[java.lang.Double]] {

  override def call(features: Seq[java.lang.Double]): Seq[java.lang.Double] = {

    val inputTensorData = features.map(_.toFloat).toArray
    val tensor = OnnxTensor.createTensor(
      ONNXSession.env,
      java.nio.FloatBuffer.wrap(inputTensorData),
      Array(1L, inputTensorData.length.toLong)
    )

    try {
      val results = ONNXSession.session.run(Map("input" -> tensor).asJava)
      val output = results.get(0).getValue.asInstanceOf[Array[Array[Float]]]
      output(0)
        .map(Float.box)
        .map(f => java.lang.Double.valueOf(f.toDouble))
        .toSeq
    } finally {
      tensor.close() // libera memoria del tensor
    }
  }
}



// package org.dmlaran

// import org.apache.spark.sql.api.java.UDF1
// import ai.onnxruntime._
// import org.apache.spark.SparkFiles
// import scala.collection.JavaConverters._

// object ONNXSingleton {
//   // OrtEnvironment y OrtSession se crean solo una vez por ejecutor
//   lazy val env: OrtEnvironment = OrtEnvironment.getEnvironment

//   lazy val session: OrtSession = {
//     val modelPath = SparkFiles.get("encoderV2.onnx")
//     val opts = new OrtSession.SessionOptions()
//     opts.setIntraOpNumThreads(1)
//     opts.setInterOpNumThreads(1)
//     env.createSession(modelPath, opts)
//   }
// }

// class AutoencoderUDF extends UDF1[Seq[java.lang.Double], Seq[java.lang.Double]] {

//   override def call(features: Seq[java.lang.Double]): Seq[java.lang.Double] = {
//     val inputTensorData = features.map(_.toFloat).toArray
//     val tensor = OnnxTensor.createTensor(
//       ONNXSingleton.env,
//       java.nio.FloatBuffer.wrap(inputTensorData),
//       Array(1L, inputTensorData.length.toLong)
//     )

//     try {
//       val results = ONNXSingleton.session.run(Map("input" -> tensor).asJava)
//       val output = results.get(0).getValue.asInstanceOf[Array[Array[Float]]]
//       output(0)
//         .map(Float.box)
//         .map(f => java.lang.Double.valueOf(f.toDouble))
//         .toSeq
//     } finally {
//       tensor.close() // liberar memoria del tensor
//     }
//   }
// }
