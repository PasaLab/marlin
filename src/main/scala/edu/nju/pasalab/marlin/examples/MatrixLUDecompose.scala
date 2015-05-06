package edu.nju.pasalab.marlin.examples

import breeze.linalg.{ DenseMatrix => BDM }
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{ SparkContext, SparkConf }

import edu.nju.pasalab.marlin.matrix.BlockID
import edu.nju.pasalab.marlin.utils.MTUtils
import edu.nju.pasalab.marlin.matrix._

/**
 * The lu decomposition(block version) of large matrix
 */
object MatrixLUDecompose {

  def main(args: Array[String]) {
    if (args.length < 2){
      System.err.println("arguments wrong, the arguments should be " +
//        "<input path> <row number of the matrix> <column number of the matrix> <output path> <cores across the cluster>")
        "<row number of the matrix> <column number of the matrix>")
      System.exit(-1)
    }
//    val cores = args(4).toInt
    val conf = new SparkConf()//.setAppName("MarlinTest_LU")

    /** if the matrices are too large, you can set below properties to tune the Spark well */
//    conf.set("spark.storage.memoryFraction", "0.4")
//    conf.set("spark.eventLog.enabled", "true")
//    conf.set("spark.storage.blockManagerTimeoutIntervalMs", "80000")
//    conf.set("spark.default.parallelism", cores.toString)
//    conf.set("spark.shuffle.file.buffer.kb", "200")
//    conf.set("spark.akka.threads", "8")
    //    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.MyRegistrator")
//    conf.set("spark.kryoserializer.buffer.max.mb", "100")
    //    conf.set("spark.local.dir", "/data/spark_dir")
    //    conf.set("spark.shuffle.consolidateFiles", "true")

    val sc = new SparkContext(conf)
//    val raw = sc.textFile(args(0)).map(line => {
//      val li = line.split(':')
//      val values = li(1).split(',').map(_.toDouble)
//      (li(0).toLong, Vectors.dense(values))
//    }).repartition(cores)

    val rows = args(0).toInt
    val cols = args(1).toInt
    val mat = MTUtils.randomDenVecMatrix(sc, rows, cols)
//    val mat = new DenseVecMatrix(raw, args(1).toLong, args(2).toInt)
    val t0 = System.currentTimeMillis()
    val (result, p) = mat.blockLUDecomposeNewNew()

    /** the result should be a zero matrix */
    println(s"the sum of array P: ${p.sum}")
    println(s"used time: ${System.currentTimeMillis() - t0} milliseconds")
//    val result = l.multiply(u, 192).subtract(mat.rowExchange(p))
//    result.saveToFileSystem(args(3))

    sc.stop()
  }
}