package edu.nju.pasalab.examples

import edu.nju.pasalab.sparkmatrix.{Matrices, MTUtils}

import breeze.linalg.{DenseMatrix => BDM}

import org.apache.spark.{SparkContext, SparkConf}

/** When a large distributed matrix multiply a quite small matrix, broadcast the small matrix,
  * and then do the matrix multiplication
  */

object IndexMatrixMultiLocal {
  def main(args: Array[String]) {
    if (args.length < 4){
      System.err.println("arguments wrong, the arguments should be " +
        "<input file path> <matB rows> <matB cols> <parallelism> " )
      System.exit(-1)
    }

    val conf = new SparkConf()
    conf.set("spark.storage.memoryFraction", "0.2")
    conf.set("spark.storage.blockManagerTimeoutIntervalMs", "80000")
    conf.set("spark.shuffle.file.buffer.kb", "200")
    conf.set("spark.akka.threads", "8")
    conf.set("spark.broadcast.factory", "org.apache.spark.broadcast.TorrentBroadcastFactory")
    //    conf.set("spark.default.parallelism", args(3))
    //    conf.set("spark.local.dir", "/data/spark_dir")
    val sc = new SparkContext(conf)
    val ma = MTUtils.loadMatrixFile(sc, args(0), args(3).toInt)

    val mb = Matrices.fromBreeze(BDM.rand[Double](args(1).toInt, args(2).toInt))
    println("mb numRows: " + mb.numRows)
    println("mb numCols: " + mb.numCols)
    ma.rows.cache()
    println("ma rows count: " + ma.rows.count())
    val result = ma.multiplyLocal(mb)
    println("result rows num: "+result.rows.count())
    println("result rows first vector size: " + result.rows.first().vector.size)
    sc.stop()
  }
}
