package edu.nju.pasalab.marlin.examples

import edu.nju.pasalab.marlin.utils.MTUtils
import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.{SparkContext, SparkConf}


object BroadcastOptimization {
  def main(args: Array[String]) {
    if (args.length < 4) {
      println("usage: OptimizationComp <matrixA row length> <matrixA column length> <matrixB column length> <mode> ")
      println("mode 1 means each row multiply the broadcast matrix B")
      println("mode 2 means transform the row-partition to local matrix and multiply the broadcast matrix B")
      System.exit(1)
    }

    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt
    val mode = args(3).toInt

    val conf = new SparkConf()
    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.BLAS2Registrator")
    val sc = new SparkContext(conf)
    println(s"arguments: ${args.mkString(" ")}")

    val matA = MTUtils.randomDenVecMatrix(sc, rowA, colA)
    val matB = BDM.rand[Double](rowB, colB)
    val t0 = System.currentTimeMillis()
    if (mode == 1){
      val result = matA.multiplyBroadcast(matB)
      result.rows.count()
    }else {
      val result = matA.oldMultiplyByRow(matB)
      result.rows.count()
    }
    println(s"in mode ${mode}, used time ${System.currentTimeMillis() - t0} millis")
    sc.stop()
  }
}
