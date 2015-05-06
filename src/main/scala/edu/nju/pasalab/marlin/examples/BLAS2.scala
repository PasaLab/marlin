package edu.nju.pasalab.marlin.examples

import edu.nju.pasalab.marlin.matrix.DistributedVector
import edu.nju.pasalab.marlin.utils.MTUtils

import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.{SparkContext, SparkConf}


object BLAS2 {
  def main(args: Array[String]) {
    if (args.length < 3){
      println("usage: BLAS2 <matrix row length> <matrix column length> <split m> <split k>")
      System.exit(1)
    }
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val rowLen = args(0).toInt
    val colLen = args(1).toInt
    val m = args(2).toInt
    val k = args(3).toInt
    val matrix = MTUtils.randomDenVecMatrix(sc, rowLen, colLen)
    val vector = BDV.rand[Double](colLen)
    println("case (a).1 collect the matrix to local ")
    var t0 = System.currentTimeMillis()
    try {
      val result1 = matrix.toBreeze() * vector
    }catch 
    println(s"top5 element of the result1: ${result1.slice(0, 5)}")
    println(s"case (a).1 used time ${(System.currentTimeMillis() - t0)} milliseconds")

    println("case (a).2 broadcast the vector out ")
    t0 = System.currentTimeMillis()
    val result2 = matrix.multiply(vector, m)
    println(s"the split num of the result2: ${result2.splitNum}")
    println(s"case (a).2 used time ${(System.currentTimeMillis() - t0)} milliseconds")

    println("case (a).3 split matrix along two dimensions")
    val distVector = DistributedVector.fromVector(sc, vector, k)
    t0 = System.currentTimeMillis()
    val result3 = matrix.multiplySpark(distVector, (m, k))
    println(s"the split num of the result3: ${result3.splitNum}")
    println(s"case (a).3 used time ${(System.currentTimeMillis() - t0)} milliseconds")

    println("case (a).4 split matrix along the column")
    t0 = System.currentTimeMillis()
    val result4 = matrix.multiplySpark(distVector, (1, k))
    println(s"the split num of the result4: ${result4.splitNum}")
    println(s"case (a).4 used time ${(System.currentTimeMillis() - t0)} milliseconds")

    sc.stop()
  }

}
