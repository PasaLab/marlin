package edu.nju.pasalab.marlin.examples

import java.util.Calendar

import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}


object CPMM {
  def main(args: Array[String]) {
    if (args.length < 6) {
      println("usage: CPMM <matrixA row length> <matrixA column length> <matrixB column length> <m> <k> <n> ")
      println("for example: CPMM 10000 10000 10000 5 5 1 ")
      System.exit(1)
    }
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt
    val m = args(3).toInt
    val k = args(4).toInt
    val n = args(5).toInt
    println(s"matrixA: $rowA by $colA ; matrixB: $rowB by $colB; " +
      s"splitMethod: $m, $k, $n, ${Calendar.getInstance().getTime}")
    val matrixA = MTUtils.randomDenVecMatrix(sc, rowA, colA)
    val matrixB = MTUtils.randomDenVecMatrix(sc, rowA, colA)
    val t0 = System.currentTimeMillis()
    val result = matrixA.cpmm(matrixB, (m, k, n))
    result.getBlocks.count
    println(s"multiplication in cpmm used time ${(System.currentTimeMillis() - t0)} millis " +
      s";${Calendar.getInstance().getTime}")
    sc.stop()
  }

}
