package edu.nju.pasalab.marlin.examples

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
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
    if (args.length < 4){
      System.err.println("arguments wrong, the arguments should be " +
        "<matrix A rows> <matrix A columns/ matrix B rows> <matrix B columns> <cores across the cluster>  " +
        "+ optional parameter{<broadcast threshold>}")
      System.exit(-1)
    }
    val conf = new SparkConf()
    /**if the matrices are too large, you can set below properties to tune the Spark well*/
//    conf.set("spark.storage.memoryFraction", "0.4")
//    conf.set("spark.eventLog.enabled", "true")
//    conf.set("spark.storage.blockManagerTimeoutIntervalMs", "80000")
//    conf.set("spark.default.parallelism", "128")
//    conf.set("spark.shuffle.file.buffer.kb", "200")
//    conf.set("spark.akka.threads", "8")
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.MyRegistrator")
//    conf.set("spark.kryoserializer.buffer.max.mb", "100")
//    conf.set("spark.local.dir", "/data/spark_dir")
//    conf.set("spark.shuffle.consolidateFiles", "true")

    val sc = new SparkContext(conf)
    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt
    val ma = MTUtils.randomDenVecMatrix(sc, rowA, colA)
    val mb = MTUtils.randomDenVecMatrix(sc, rowB, colB)
    val threshold = if (args.length < 5) {
      300
    }else { args(5).toInt }
    val result = ma.multiply(mb, args(3).toInt, threshold)
    println("Result RDD counts: " + result.elementsCount())
    sc.stop()
  }
}


class MyRegistrator extends KryoRegistrator{
  override def registerClasses(kryo: Kryo){
    kryo.register(classOf[BlockID])
    kryo.register(classOf[BDM[Double]])
    kryo.register(classOf[BDV[Double]])
  }
}