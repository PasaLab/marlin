package edu.nju.pasalab.marlin.examples

import edu.nju.pasalab.marlin.utils.MTUtils

import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.{SparkContext, SparkConf}


object BLAS3Broadcast {
  def main(args: Array[String]) {
    if (args.length < 3) {
      println("usage: BLAS3Broadcast <matrixA row length> <matrixA column length> <matrixB column length> <split m> <mode>")
      println("for example: BLAS3Broadcast 500000 100000 100 ")
      System.exit(1)
    }
    val conf = new SparkConf()
    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.BLAS2Registrator")
    val sc = new SparkContext(conf)
    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt
    val matrixA = MTUtils.randomDenVecMatrix(sc, rowA, colA)
    //    val matrixB = MTUtils.randomDenVecMatrix(sc, rowB, colB)
    val matrixB = BDM.rand[Double](rowB, colB)
    val t0 = System.currentTimeMillis()
    //    val result = matrixA.multiplyBroadcast(matrixB, splitM)
    //    result.blocks.count()
    //    println(s"multiplication toBlockMatrix and broadcast used time ${(System.currentTimeMillis() - t0)} milliseconds")
    //    t0 = System.currentTimeMillis()
//    val result2 = matrixA.multiplyByRow(matrixB)
//    result2.rows.count()
//    println(s"multiplication DenseVecMatrix and broadcast used time ${(System.currentTimeMillis() - t0)} milliseconds")
    if (args.length > 3){
      val result3 = matrixA.multiplyBroadcast(matrixB, args(3).toInt)
      result3.blocks.count()
    }else {
      val result3 = matrixA.multiplyBroadcastNew(matrixB)
      result3.rows.count()
    }
    println(s"arguments: $args")
    println(s"multiplication DenseVecMatrix and broadcast used time ${(System.currentTimeMillis() - t0)} milliseconds")

    sc.stop()

  }
}
