package edu.nju.pasalab.marlin.examples

import breeze.linalg.{DenseVector => BDV}
import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}

object BLAS2NoBlock {
  def main(args: Array[String]) {
    if (args.length < 2){
      println("usage: BLAS2NoBlock <matrix row length> <matrix column length> ")
      System.exit(1)
    }
    val conf = new SparkConf()
    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.BLAS2Registrator")
    val sc = new SparkContext(conf)
    val rowLen = args(0).toInt
    val colLen = args(1).toInt
    val matrix = MTUtils.randomDenVecMatrix(sc, rowLen, colLen)
    val vector = BDV.rand[Double](colLen)
    println(s"arguments: ${args.mkString(" ")}")
    println("======================================")
    println(s"local vector length: ${vector.length}")
    val t0 = System.currentTimeMillis()
    println("case (a).2 broadcast the vector out ")
    val result = matrix.multiplyNoBLock(vector)
    println(s"the element 0 of the result: ${result(0)}")
    println(s"multiplication withou block used time ${(System.currentTimeMillis() - t0)} milliseconds")
    sc.stop()

  }
}
