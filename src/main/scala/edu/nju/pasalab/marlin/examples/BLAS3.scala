package edu.nju.pasalab.marlin.examples

import java.util.Calendar

import edu.nju.pasalab.marlin.utils.MTUtils

import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.{SparkContext, SparkConf}


object BLAS3 {
  def main(args: Array[String]) {
    if (args.length < 4) {
      println("usage: BLAS3 <matrixA row length> <matrixA column length> <matrixB column length> <mode> <m> <k> <n> ")
      println("mode 1 means collect the two matrix to local, and then execute multiplication")
      println("mode 2 means broadcast one of the matrix out, and then execute multiplication")
      println("mode 3 means shuffle the two distributed matrix, and then execute multiplication")
      println("for example: BLAS3 10000 10000 10000 1 5 5 5 ")
      System.exit(1)
    }
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt

    val mode = args(3).toInt
    println(s"matrixA: $rowA by $colA ; matrixB: $rowB by $colB, mode: $mode ;${Calendar.getInstance().getTime}")
    if (mode == 1) {
      val t0 = System.currentTimeMillis()
      val matA = BDM.rand[Double](rowA, colA)
      val matB = BDM.rand[Double](rowB, colB)
      val rsult = matA * matB
      println(s"no breeze, local multiplication in mode $mode used time ${(System.currentTimeMillis() - t0)} millis " +
        s";${Calendar.getInstance().getTime}")
    }else if (mode == 2) {
      val m = args(4).toInt
      println(s"RowMatrix initialize with $m partitions ")
      val matrixA = MTUtils.randomDenVecMatrix(sc, rowA, colA, m)
      val matrixB = BDM.rand[Double](rowB, colB)
      val t0 = System.currentTimeMillis()
      val result = matrixA.multiply(matrixB)
      result.rows.count
      println(s"multiplication in mode $mode used time ${(System.currentTimeMillis() - t0)} millis " +
        s";${Calendar.getInstance().getTime}")
    }else if (mode ==3) {
      val m = args(4).toInt
      val k = args(5).toInt
      val n = args(6).toInt
      val matrixA = MTUtils.randomDenVecMatrix(sc, rowA, colA)
      val matrixB = MTUtils.randomDenVecMatrix(sc, rowB, colB)
      val t0 = System.currentTimeMillis()
      val result = matrixA.multiply(matrixB, (m, k, n))
      result.blocks.count
      println(s"multiplication in mode $mode used time ${(System.currentTimeMillis() - t0)} millis " +
        s";${Calendar.getInstance().getTime}")
    }
    sc.stop()

  }
}
