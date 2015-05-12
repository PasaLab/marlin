package edu.nju.pasalab.marlin.examples

import breeze.linalg.{DenseVector => BDV}
import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}

object BLAS2NoBlock {
  def main(args: Array[String]) {
    if (args.length < 3){
      println("usage: BLAS2NoBlock <matrix row length> <matrix column length> <which way to multiply>")
      println("usage: BLAS2NoBlock 100000 10000 NoMatrix")
      System.exit(1)
    }
    val conf = new SparkConf()
    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.BLAS2Registrator")
    val sc = new SparkContext(conf)
    val rowLen = args(0).toInt
    val colLen = args(1).toInt
    val mode = args(2)
    val matrix = MTUtils.randomDenVecMatrix(sc, rowLen, colLen, 500)
    val vector = BDV.rand[Double](colLen)
    println(s"arguments: ${args.mkString(" ")}")
    println("======================================")
    println(s"local vector length: ${vector.length}")
    if (mode.toLowerCase.equals("nomatrix")) {
      val t0 = System.currentTimeMillis()
      println("broadcast the vector out no sub-matrix")
      val result = matrix.multiplyVector(vector)
      println(s"the element 0 of the result: ${result(0)}")
      println(s"multiplication used time ${(System.currentTimeMillis() - t0)} milliseconds")
    }else {
      val t0 = System.currentTimeMillis()
      println("broadcast the vector out with sub-matrix")
      val result2 = matrix.multiplyVector2(vector)
      println(s"the element 0 of the result2: ${result2(0)}")
      println(s"multiplication with sub-matrix used time ${(System.currentTimeMillis() - t0)} milliseconds")
    }
    sc.stop()

  }
}
