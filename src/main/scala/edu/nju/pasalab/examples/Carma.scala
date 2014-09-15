package edu.nju.pasalab.examples

import edu.nju.pasalab.sparkmatrix.MTUtils
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Two large matrices multiply with CARMA algorithm
 */
object Carma {

  def main(args: Array[String]) {
    if (args.length < 4){
      System.err.println("arguments wrong, the arguments should be " +
        "<input file path A> <input file path B> <output file path> <cores cross the cluster> ")
      System.exit(-1)
    }
    val conf = new SparkConf().setAppName("Carma multiply")
    conf.set("spark.storage.memoryFraction", "0.5")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.storage.blockManagerTimeoutIntervalMs", "80000")
    conf.set("spark.default.parallelism", args(3))
    conf.set("spark.shuffle.file.buffer.kb", "200")
    conf.set("spark.akka.threads", "8")
//    conf.set("spark.local.dir", "/data/spark_dir")
    val sc = new SparkContext(conf)
    val ma = MTUtils.loadMatrixFile(sc, args(0))
    val mb = MTUtils.loadMatrixFile(sc, args(1))
    val (m, k , n) = MTUtils.splitMethod(ma.numRows(), ma.numCols(), mb.numCols(), args(3).toInt)
    println("the split method: " + m + ", "+ k + ", "+ n )
    val result = ma.multiplyCarma(mb, args(3).toInt)
    result.saveToFileSystem(args(2))

  }
}
