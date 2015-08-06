package edu.nju.pasalab.marlin.examples

import java.util.Calendar

import breeze.linalg.DenseMatrix
import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}
import breeze.linalg.{DenseMatrix => BDM}

object BLAS3shuffle {
  def main(args: Array[String]) {
    if (args.length < 4) {
      println("usage: BLAS3shuffle <matrixA row length> <matrixA column length> <matrixB column length> <mode> <m> <k> <n> ")
      println("mode 1 means use shuffle avoid strategy")
      println("mode 2 means compute first strategy")
      println("mode 3 means multiplyReduceShuffle2")
      println("mode 4 means multiplyReduceShuffle3")
      println("for example: BLAS3shuffle 10000 10000 10000 1 5 5 5 ")
      System.exit(1)
    }
    val conf = new SparkConf().setMaster("local[*]").setAppName("test partitioner")
    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.BLAS2Registrator")
    conf.set("spark.default.parallelism", "8")
    val sc = new SparkContext(conf)
    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt

    val mode = args(3).toInt
    println(s"matrixA: $rowA by $colA ; matrixB: $rowB by $colB, mode: $mode ;${Calendar.getInstance().getTime}")
    val m = args(4).toInt
    val k = args(5).toInt
    val n = args(6).toInt
    val matrixA = MTUtils.randomDenVecMatrix(sc, rowA, colA)
    val matrixB = MTUtils.randomDenVecMatrix(sc, rowB, colB)
    if (mode == 1) {
      val t0 = System.currentTimeMillis()
      val result = matrixA.multiplyReduceShuffle(matrixB, (m, k, n))
      result.blocks.count
      result.print()
      println(s"multiplication in mode $mode used time ${(System.currentTimeMillis() - t0)} millis " +
        s";${Calendar.getInstance().getTime}")
    }else if (mode == 2) {
      val t0 = System.currentTimeMillis()
      val result = matrixA.multiply(matrixB, (m, k, n))
      result.blocks.count
      result.print()
      println(s"multiplication in mode $mode used time ${(System.currentTimeMillis() - t0)} millis " +
        s";${Calendar.getInstance().getTime}")
    }else if (mode == 3){
      val t0 = System.currentTimeMillis()
      val result = matrixA.multiplyReduceShuffle2(matrixB, (m, k, n))
      println(s"result blocks: ${result.blocks.count}")
      result.print()
      println(s"multiplication in mode $mode used time ${(System.currentTimeMillis() - t0)} millis " +
        s";${Calendar.getInstance().getTime}")
    }else {
      val t0 = System.currentTimeMillis()
      val result = matrixA.multiplyReduceShuffle3(matrixB, (m, k, n))
      result.blocks.count
      println(s"multiplication in mode $mode used time ${(System.currentTimeMillis() - t0)} millis " +
        s";${Calendar.getInstance().getTime}")
    }
    sc.stop()

  }
}
