package edu.nju.pasalab.marlin.examples

import breeze.linalg.{DenseMatrix => BDM}
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkContext, SparkConf}

import edu.nju.pasalab.marlin.matrix.BlockID
import edu.nju.pasalab.marlin.utils.MTUtils

/**
 * Two large matrices multiply example
 */
object MatrixMultiply {

  def main(args: Array[String]) {
    if (args.length < 5){
      System.err.println("arguments wrong, the arguments should be " +
        "<matrix A rows length> <matrix A columns length> <matrix B columns length> <cores across the cluster> <output path> " +
        "+ optional parameter{<broadcast threshold>}")
      System.exit(-1)
    }
    val conf = new SparkConf()
    /**if the matrices are too large, you can set below properties to tune the Spark well*/
    conf.set("spark.storage.memoryFraction", "0.4")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.storage.blockManagerTimeoutIntervalMs", "80000")
    conf.set("spark.default.parallelism", (2*args(3).toInt).toString)
    conf.set("spark.shuffle.file.buffer.kb", "200")
    conf.set("spark.akka.threads", "8")
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.MyRegistrator")
    conf.set("spark.kryoserializer.buffer.max.mb", "100")
//    conf.set("spark.local.dir", "/data/spark_dir")
//    conf.set("spark.shuffle.consolidateFiles", "true")

    val sc = new SparkContext(conf)
    //load matrices from file, args(0) is the input matrix file A, args(1) is the input matrix file B
//    val ma = MTUtils.loadMatrixFile(sc, args(0), args(2).toInt)
//    val mb = MTUtils.loadMatrixFile(sc, args(1), args(2).toInt)
    val ma = MTUtils.randomDenVecMatrix(sc, args(0).toInt, args(1).toInt)
    val mb = MTUtils.randomDenVecMatrix(sc, args(1).toInt, args(2).toInt)
    val threshold = if (args.length < 6) {
      300
    }else { args(5).toInt }
    val result = ma.multiply(mb, args(3).toInt, threshold)
    println("Result RDD counts: " + result.blocks.count())
    println("start store the result matrix in DenseVecMatrix type")
    result.saveToFileSystem(args(4))
    sc.stop()
  }
}


class MyRegistrator extends KryoRegistrator{
  override def registerClasses(kryo: Kryo){
    kryo.register(classOf[BlockID])
    kryo.register(classOf[BDM[Double]])
  }
}