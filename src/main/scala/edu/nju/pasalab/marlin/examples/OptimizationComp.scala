package edu.nju.pasalab.marlin.examples

import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}


object OptimizationComp {
  def main(args: Array[String]) {
    if (args.length < 7) {
      println("usage: OptimizationComp <matrixA row length> <matrixA column length> <matrixB column length> " +
        "<mode> <m> <k> <n> ")
      println("mode 1 means the baseline implementation, coordinate-matrix to block-matrix")
      println("mode 2 means with transformation optimization")
      println("mode 3 means with joinBroadcast optimization")
      println("mode 4 means with all the optimization")
      System.exit(1)
    }

    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt
    val mode = args(3).toInt
    val m = args(4).toInt
    val k = args(5).toInt
    val n = args(6).toInt
    val conf = new SparkConf()
    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.BLAS2Registrator")
    val sc = new SparkContext(conf)
    println(s"arguments: ${args.mkString(" ")}")
    val matA = MTUtils.randomDenVecMatrix(sc, rowA, colA)
    val matB = MTUtils.randomDenVecMatrix(sc, rowB, colB)
    val t0 = System.currentTimeMillis()
    if (mode == 1){
      val result = matA.multiplyCoordinateBlock(matB, (m, k, n))
      result.blocks.count()
    }else if (mode == 2){
      val result = matA.multiply(matB, (m, k, n))
      result.blocks.count()
    }else if (mode == 3){
      val result = matA.multiplyCBjoinBroadcast(matB, (m, k, n))
      result.blocks.count()
    }else {
      val result = matA.multiplyOptimize(matB, (m, k, n))
      result.blocks.count()
    }
    println(s"in mode ${mode}, used time ${System.currentTimeMillis() - t0} millis")
    sc.stop()
  }
}
