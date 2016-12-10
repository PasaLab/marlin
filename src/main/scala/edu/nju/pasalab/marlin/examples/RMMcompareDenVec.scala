package edu.nju.pasalab.marlin.examples

import java.util.Calendar

import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}


object RMMcompareDenVec {
  def main(args: Array[String]) {
    if (args.length < 7) {
      println("usage: RMMcompareDenVec <matrixA row length> <matrixA column length> <matrixB column length> <mode> <m> <k> <n>")
      println("mode 1 means RMMv2 from DenVecMatrix")
      println("mode 2 means RMMv2 from DenVecMatrix with JoinBroadcast")
      println("mode 3 means RMMv2 transform by coordinate matrix")
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
    val parallelism = if(args.length > 7) args(7).toInt else 320
    println(s"Marlin RMM-opt fom DenseVecMatrix, matrixA: $rowA by $colA ; matrixB: $rowB by $colB, mode: $m, $k, $n. parallelism: $parallelism" +
      s" ${Calendar.getInstance().getTime}")
    val rowMatA = MTUtils.randomDenVecMatrix(sc, rowA, colA, parallelism)
    val rowMatB = MTUtils.randomDenVecMatrix(sc, rowB, colB, parallelism)
    val t0 = System.currentTimeMillis()

    mode match {
      case 1 =>
        val matA = rowMatA.toBlockMatrix(m, k)
        val matB = rowMatB.toBlockMatrix(k, n)
        val result = matA.multiply(matB)
        MTUtils.evaluate(result.blocks)
        println(s"Marlin RMM-opt fom DenseVecMatrix used time ${(System.currentTimeMillis() - t0)} millis " +
          s";${Calendar.getInstance().getTime}")
      case 2 =>
        val matA = rowMatA.toBlockMatrix(m, k)
        val matB = rowMatB.toBlockMatrix(k, n)
        val result = matA.multiplyJoinBroadcast(matB)
        MTUtils.evaluate(result.blocks)
        println(s"!!!Marlin RMM-opt fom DenseVecMatrix with JoinBroadcast used time ${(System.currentTimeMillis() - t0)} millis " +
          s";${Calendar.getInstance().getTime}")
      case 3 =>
        val result = rowMatA.multiplyCoordinateBlock(rowMatB, (m, k, n))
        MTUtils.evaluate(result.blocks)
        println(s"!!!Marlin RMM-opt fom DenseVecMatrix transform by coordinate matrix used time ${(System.currentTimeMillis() - t0)} millis " +
          s";${Calendar.getInstance().getTime}")
    }
    println("=========================================")
    sc.stop()

  }
}
