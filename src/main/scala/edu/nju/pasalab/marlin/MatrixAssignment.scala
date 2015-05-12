package edu.nju.pasalab.marlin

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}

import scala.util.Random

object MatrixAssignment {
  def main(args: Array[String]) {
    if (args.length < 3){
      println("usage: MatrixAssignment <matrix row length> <matrix column length> <another matrix column>")
      System.exit(1)
    }
    val rows = args(0).toInt
    val cols, rowB = args(1).toInt
    val colB = args(2).toInt
    val other = BDM.rand[Double](rowB, colB)
    val vectors = Iterator.tabulate[BDV[Double]](10)(i => BDV.rand[Double](cols)).toArray
    val matrixT = BDM.zeros[Double](cols, rows)
    println("initiation completed!")
    var t0 = System.currentTimeMillis()
    for (i <- 0 until cols) {
      matrixT(::, i) := vectors(Random.nextInt(10))
    }
    val result = matrixT.t
    val finalResult: BDM[Double] = result * other
    println(s" final matrix row 1 first 5 element: ${finalResult(::, 1).slice(0, 5)}")
    println(s"matrix transpose assignment and multiplication used time: ${System.currentTimeMillis() - t0} millis")
    val matrix = BDM.zeros[Double](rows, cols)
    t0 = System.currentTimeMillis()
    for (i <- 0 until rows) {
      matrix(i, ::) := vectors(Random.nextInt(10)).t
    }
    val finalResult2: BDM[Double] = matrix * other
    println(s" final matrix2 row 1 first 5 element: ${finalResult2(::, 1).slice(0, 5)}")
    println(s"matrix assignment used time: ${System.currentTimeMillis() - t0} millis")

  }

}
