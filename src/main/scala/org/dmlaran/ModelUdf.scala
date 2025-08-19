package org.dmlaran

import org.apache.spark.sql.api.java.UDF1
import ai.onnxruntime._
import org.apache.spark.SparkFiles
import scala.collection.JavaConverters._
import ai.onnxruntime.OrtSession.SessionOptions
import java.util

class AutoencoderUDF
    extends UDF1[Seq[java.lang.Double], Seq[java.lang.Double]] {

  override def call(features: Seq[java.lang.Double]): Seq[java.lang.Double] = {

    // Inicializa ONNX Runtime (puedes optimizar para no recrear cada vez)
    val env: OrtEnvironment = OrtEnvironment.getEnvironment
    val session: OrtSession = {
      val modelPath = SparkFiles.get("encoderV2.onnx")

      // Configuramos la sesión con pocos hilos
      val opts = new SessionOptions()
      opts.setIntraOpNumThreads(1)
      opts.setInterOpNumThreads(1)

      env.createSession(modelPath, opts)
    }

    val inputTensorData = features.map(_.toFloat).toArray
    val tensor = OnnxTensor.createTensor(
      env,
      java.nio.FloatBuffer.wrap(inputTensorData),
      Array(1L, inputTensorData.length.toLong)
    )

    val results = session.run(Map("input" -> tensor).asJava)
    val output = results.get(0).getValue.asInstanceOf[Array[Array[Float]]]

    output(0)
      .map(Float.box)
      .map(f => java.lang.Double.valueOf(f.toDouble))
      .toSeq
  }
}

// object AutoencoderModel {

//   // Entorno ONNX
//   val env: OrtEnvironment = OrtEnvironment.getEnvironment

//   // Sesión del modelo
//   val session: OrtSession = {
//     val modelPath = "src/main/resources/encoderV2.onnx"
//     env.createSession(modelPath, new OrtSession.SessionOptions())
//   }

//   /** UDF que recibe un vector completo de características (numéricas +
//     * categóricas ya one-hot) y devuelve la reconstrucción del autoencoder
//     */
//   val autoencoderUDF: UserDefinedFunction = udf { (features: Seq[Double]) =>
//     // Convierte a FloatBuffer
//     val inputTensorData = features.map(_.toFloat).toArray
//     val tensor = OnnxTensor.createTensor(
//       env,
//       java.nio.FloatBuffer.wrap(inputTensorData),
//       Array(1L, inputTensorData.length.toLong) // batch=1, input_dim
//     )

//     // Ejecuta la inferencia
//     val results = session.run(Map("input" -> tensor).asJava)
//     val output = results.get(0).getValue.asInstanceOf[Array[Array[Float]]]

//     // Devuelve como Seq[Double]
//     output(0).map(_.toDouble)
//   }
// }
