package edu.nju.pasalab.marlin.examples

import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}

object BLAS3New {
  def main(args: Array[String]) {
    if (args.length < 4) {
      println("usage: BLAS3New <matrixA row length> <matrixA column length> <matrixB column length> <mode> <m> <k> <n> ")
      println("mode 1 means using joinBroadcast optimization")
      println("for example: BLAS3New 10000 10000 10000 1 5 5 5 ")
      System.exit(1)
    }
    val conf = new SparkConf()
    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.BLAS2Registrator")
    val sc = new SparkContext(conf)
    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt
    val mode = args(3).toInt
    if (mode == 1) {
      println(s"arguments: $args")
      val m = args(4).toInt
      val k = args(5).toInt
      val n = args(6).toInt
      val matrixA = MTUtils.randomDenVecMatrix(sc, rowA, colA)
      val matrixB = MTUtils.randomDenVecMatrix(sc, rowB, colB)
      val t0 = System.currentTimeMillis()
      val result = matrixA.multiplyOptimize(matrixB, (m, k, n))
      result.blocks.count
      println(s"multiplication in mode $mode used time ${(System.currentTimeMillis() - t0)} milliseconds")
    }
    sc.stop()
  }
}
