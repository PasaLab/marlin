package edu.nju.pasalab.marlin.examples

import edu.nju.pasalab.marlin.utils.MTUtils

import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.{SparkContext, SparkConf}


object BLAS3NoBlock {
  def main(args: Array[String]) {
    if (args.length < 4){
      println("arguments: <matrixA row length> <matrixA column length> <matrixB column length> <blocks>")
      System.exit(1)
    }
    val conf = new SparkConf()
    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.BLAS2Registrator")
    val sc = new SparkContext(conf)
    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt
    val matrixA = MTUtils.randomDenVecMatrix(sc, rowA, colA, 80)
    matrixA.rows.count()
    val matrixB = BDM.rand[Double](rowB, colB)
    var t0 = System.currentTimeMillis()
    val result1 = matrixA.oldMultiplyByRow(matrixB)
    result1.rows.count()
    println(s"NoBlock multiplication used time ${(System.currentTimeMillis() - t0)} milliseconds")
    t0 = System.currentTimeMillis()
    val result2 = matrixA.toBlockMatrix(args(3).toInt, 1).multiply(matrixB)
    result2.blocks.count()
    println(s"Block multiplication used time ${(System.currentTimeMillis() - t0)} milliseconds")
    sc.stop()
  }

}
