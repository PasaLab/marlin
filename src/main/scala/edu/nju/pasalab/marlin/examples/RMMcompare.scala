package edu.nju.pasalab.marlin.examples

import java.util.Calendar

import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}


object RMMcompare {
  def main(args: Array[String]) {
    if (args.length < 4) {
      println("usage: RMMcompare <matrixA row length> <matrixA column length> <matrixB column length> <mode> <m> <k> <n>")
      println("mode 1 means basic RMM")
      println("mode 2 means RMMv2")
      println("mode 3 means RMMv3 with joinBroadcast")
      println("for example: RMMcompare 30000 30000 30000 1 6 6 6")
      println("*** NOTES, only support RMM-opt")
      System.exit(1)
    }
    val conf = new SparkConf()
    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.BLAS1Registrator")
    val sc = new SparkContext(conf)
    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt
    val mode = args(3).toInt
    val m = args(4).toInt
    val k = args(5).toInt
    val n = args(6).toInt
    val matrixA = MTUtils.randomBlockMatrix(sc, rowA, colA, m, k)
    // this step used to distribute blocks uniformly across the custer
    matrixA.blocks.count()
    val matrixB = MTUtils.randomBlockMatrix(sc, rowB, colB, k, n)
    // this step used to distribute blocks uniformly across the custer
    matrixB.blocks.count()
    println("=========================================")
    println(s"RMMcompare matrixA: $rowA by $colA ; matrixB: $rowB by $colB, mode: $mode " +
      s"m, k, n: $m, $k, $n; ${Calendar.getInstance().getTime}")
    mode match {
      case 1 =>
//        val t0 = System.currentTimeMillis()
//        val result = matrixA.rmm(matrixB)
//        MTUtils.evaluate(result.blocks)
//        println(s"RMM in mode $mode used time ${(System.currentTimeMillis() - t0)} millis " +
//          s";${Calendar.getInstance().getTime}")
      case 2 =>
        val t0 = System.currentTimeMillis()
        val result = matrixA.multiply(matrixB)
        MTUtils.evaluate(result.blocks)
        println(s"RMMv2 in mode $mode used time ${(System.currentTimeMillis() - t0)} millis " +
          s";${Calendar.getInstance().getTime}")
      case 3 =>
//        val t0 = System.currentTimeMillis()
//        val result = matrixA.multiplyJoinBroadcast(matrixB)
//        MTUtils.evaluate(result.blocks)
//        println(s"RMMv3 with joinBroadcast in mode $mode used time ${(System.currentTimeMillis() - t0)} millis " +
//          s";${Calendar.getInstance().getTime}")
    }
    println("=========================================")
    sc.stop()
  }
}
