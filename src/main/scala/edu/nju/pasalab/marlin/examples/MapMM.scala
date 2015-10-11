package edu.nju.pasalab.marlin.examples

import java.util.Calendar

import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}


object MapMM {
  def main(args: Array[String]) {
    if (args.length < 4) {
      println("usage: MapMM <matrixA row length> <matrixA column length> <matrixB column length> <mode> ")
      println("mode 1 means collect one matrix and then broadcast out to finish MapMM")
      println("mode 2 means using executorBroadcast to finish MapMM")
      println("for example: MapMM 10000 10000 10000 1")
      System.exit(1)
    }
    val conf = new SparkConf()
    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.BLAS1Registrator")
    val sc = new SparkContext(conf)
    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt
    val mode = args(3).toInt
    val matrixA = MTUtils.randomDenVecMatrix(sc, rowA, colA)
    println(s"matrixA: $rowA by $colA ; matrixB: $rowB by $colB, mode: $mode ;${Calendar.getInstance().getTime}")
    mode match {
      case 1 =>
        val matrixB = MTUtils.randomDenVecMatrix(sc, rowB, colB)
        val t0 = System.currentTimeMillis()
        val mat = matrixB.toBreeze()
        val result = matrixA.multiply(mat)
        result.elementsCount()
        println(s"MapMM in mode $mode used time ${(System.currentTimeMillis() - t0)} millis " +
          s";${Calendar.getInstance().getTime}")
      case 2 =>
        val matrixB = MTUtils.randomDenVecMatrix(sc, rowB, colB, 12)
        val t0 = System.currentTimeMillis()
        val result = matrixA.mapmm(matrixB)
        result.elementsCount()
        println(s"MapMM in mode $mode used time ${(System.currentTimeMillis() - t0)} millis " +
          s";${Calendar.getInstance().getTime}")
    }
    sc.stop()
  }
}
