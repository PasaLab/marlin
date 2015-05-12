package edu.nju.pasalab.marlin.examples

import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}


object BLAS3 {
  def main(args: Array[String]) {
    if (args.length < 7) {
      println("usage: BLAS3 <matrixA row length> <matrixA column length> <matrixB column length> <m> <k> <n> <mode>")
      println("for example: BLAS3 10000 10000 10000 5 5 5 new")
      System.exit(1)
    }
    val conf = new SparkConf()
    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.BLAS2Registrator")
    val sc = new SparkContext(conf)
    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt
    val matrixA = MTUtils.randomDenVecMatrix(sc, rowA, colA)
    val matrixB = MTUtils.randomDenVecMatrix(sc, rowB, colB)
    val m = args(3).toInt
    val k = args(4).toInt
    val n = args(5).toInt
    val mode = args(6)
    println(s"split mode: ($m , $k, $n); matrixA: $rowA by $colA ; matrixB: $rowB by $colB, mode: $mode")
    if (mode.equals("new")) {
      val t0 = System.currentTimeMillis()
      val result = matrixA.multiply(matrixB, (m , k , n))
      result.blocks.count()
      println(s"multiplication used time ${(System.currentTimeMillis() - t0)} milliseconds")
    }else {
      val t0 = System.currentTimeMillis()
      val result = matrixA.oldMultiplySpark(matrixB, (m, k, n))
      result.blocks.count()
      println(s"multiplication used time ${(System.currentTimeMillis() - t0)} milliseconds")
    }
    sc.stop()

  }
}
