package edu.nju.pasalab.marlin.examples

import java.util.Calendar

import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}


object CPMMsingle {
  def main(args: Array[String]) {
    if (args.length < 6) {
      println("usage: CPMMsingle <matrixA row length> <matrixA column length> <matrixB column length> <mode> <m> <k> <n> ")
      println("mode 1 means original reduce, while 2 means treeReduce")
      println("for example: CPMM 10000 1000 10000 1 5 1 5 ")
      System.exit(1)
    }
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt
    val mode = args(3).toInt
    val m = args(4).toInt
    val k = args(5).toInt
    val n = args(6).toInt
    println("=========================================")
    println(s"matrixA: $rowA by $colA ; matrixB: $rowB by $colB; " +
      s"splitMethod: $m, $k, $n, ${Calendar.getInstance().getTime}")
    val matrixA = MTUtils.randomBlockMatrix(sc, rowA, colA, m, k)
    val matrixB = MTUtils.randomBlockMatrix(sc, rowB, colB, k, n)
    // this step used to distribute blocks uniformly across the custer
    matrixA.blocks.count()
    // this step used to distribute blocks uniformly across the custer
    matrixB.blocks.count()
    val t0 = System.currentTimeMillis()
   mode match {
      case 1 =>
        val result = matrixA.cpmmSingle(matrixB)
        result.elementsCount()
        println(s"cpmm original reduce used time ${(System.currentTimeMillis() - t0)} millis " +
          s";${Calendar.getInstance().getTime}")
      case 2 =>
        val result = matrixA.cpmmTre(matrixB)
        result.elementsCount()
        println(s"cpmm tree reduce used time ${(System.currentTimeMillis() - t0)} millis " +
          s";${Calendar.getInstance().getTime}")
    }

    println("=========================================")
    sc.stop()
  }
}
