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
    if (args.length < 5){
      System.err.println("arguments wrong, the arguments should be " +
        "<input file path A> <input file path B>  " +
        "<cores split num> <file partitions> <tasks nums> <output path>")
      System.exit(-1)
    }
    val conf = new SparkConf()//.setAppName("Carma multiply")
    /**if the matrices are too large, you can set below properties to tune the Spark well*/
    conf.set("spark.storage.memoryFraction", "0.3")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.storage.blockManagerTimeoutIntervalMs", "80000")
    conf.set("spark.default.parallelism", args(4))
    conf.set("spark.shuffle.file.buffer.kb", "200")
    conf.set("spark.akka.threads", "8")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.kryo.registrator", "edu.nju.pasalab.examples.MyRegistrator")
    conf.set("spark.kryoserializer.buffer.max.mb", "100")
//    conf.set("spark.local.dir", "/data/spark_dir")
//    conf.set("spark.shuffle.consolidateFiles", "true")

    val sc = new SparkContext(conf)
    val ma = MTUtils.loadMatrixFile(sc, args(0), args(3).toInt)
    val mb = MTUtils.loadMatrixFile(sc, args(1), args(3).toInt)
    val (m, k , n) = MTUtils.splitMethod(ma.numRows(), ma.numCols(), mb.numCols(), args(2).toInt)
    println("the split method: " + m + ", "+ k + ", "+ n )
    val result = ma.multiplyCarma(mb, args(2).toInt)
    println("Result RDD counts:" + result.blocks.count())
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