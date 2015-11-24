package edu.nju.pasalab.marlin.examples


import java.util.Calendar

import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}


object MapMMcompare {
  def main(args: Array[String]) {
    if (args.length < 4) {
      println("usage: MapMMcompare <matrixA row length> <matrixA column length> <matrixB column length>" +
        " <mode> <partitions>")
      println("mode 1 means basic MapMM")
      println("mode 2 means MapMMv2")
      println("for example: MapMMcompare 500000 10000 1000 1")
      System.exit(1)
    }
    val conf = new SparkConf()
    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.BLAS1Registrator")
    val sc = new SparkContext(conf)
    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt
    val mode = args(3).toInt
    val partitions = args(4).toInt
    val matrixA = MTUtils.randomDenVecMatrix(sc, rowA, colA, partitions)
    val matrixB = MTUtils.randomDenVecMatrix(sc, rowB, colB)
    println("=========================================")
    println(s"MapMMcompare: matrixA: $rowA by $colA, partitions: $partitions ; matrixB: $rowB by $colB," +
      s" mode: $mode; ${Calendar.getInstance().getTime}")
    mode match {
      case 1 =>
        val t0 = System.currentTimeMillis()
        val result = matrixA.mapmm(matrixB.toBreeze())
        MTUtils.evaluate(result.rows)
        println(s"MapMM in mode $mode used time ${(System.currentTimeMillis() - t0)} millis " +
          s";${Calendar.getInstance().getTime}")
      case 2 =>
        val t0 = System.currentTimeMillis()
        val result = matrixA.multiply(matrixB.toBreeze())
        MTUtils.evaluate(result.rows)
        println(s"MapMMv2 in mode $mode used time ${(System.currentTimeMillis() - t0)} millis " +
          s";${Calendar.getInstance().getTime}")
    }
    println("=========================================")
    sc.stop()
  }
}


