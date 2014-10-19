package edu.nju.pasalab.examples

import breeze.linalg.{DenseMatrix => BDM}
import com.esotericsoftware.kryo.Kryo
import edu.nju.pasalab.sparkmatrix.{BlockID, IndexRow, Block, MTUtils}

import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Two large matrices multiply with CARMA algorithm
 */
object MatrixMultiply {

  def main(args: Array[String]) {
    if (args.length < 3){
      System.err.println("arguments wrong, the arguments should be " +
        "<input file path A> <input file path B> <cores across the cluster> <broadcast threshold>")
      System.exit(-1)
    }
    val conf = new SparkConf()
    /**if the matrices are too large, you can set below properties to tune the Spark well*/
    conf.set("spark.storage.memoryFraction", "0.4")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.storage.blockManagerTimeoutIntervalMs", "80000")
    conf.set("spark.default.parallelism", (2*args(2).toInt).toString)
    conf.set("spark.shuffle.file.buffer.kb", "200")
    conf.set("spark.akka.threads", "8")
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.set("spark.kryo.registrator", "edu.nju.pasalab.examples.MyRegistrator")
    conf.set("spark.kryoserializer.buffer.max.mb", "100")
//    conf.set("spark.local.dir", "/data/spark_dir")
//    conf.set("spark.shuffle.consolidateFiles", "true")

    val sc = new SparkContext(conf)
    val ma = MTUtils.loadMatrixFile(sc, args(0), args(2).toInt)
    val mb = MTUtils.loadMatrixFile(sc, args(1), args(2).toInt)
    val threshold = if (args.length < 4) {
      300
    }else { args(3).toInt }
    val result = ma.multiply(mb, args(2).toInt, threshold)
    println("Result RDD counts: " + result.blocks.count())
//    val testBlk = result.blocks.first()
//    println("result first block ID: " + testBlk._1.row + " " + testBlk._1.column)
//    println("result first block (1,1): " + testBlk._2.apply(1,1))
//    result.saveToFileSystem(args(5),"blockmatrix")
    sc.stop()
  }
}


class MyRegistrator extends KryoRegistrator{
  override def registerClasses(kryo: Kryo){
    kryo.register(classOf[Block])
    kryo.register(classOf[IndexRow])
    kryo.register(classOf[BlockID])
    kryo.register(classOf[BDM[Double]])
  }
}