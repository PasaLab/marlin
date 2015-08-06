package edu.nju.pasalab.marlin.examples

import edu.nju.pasalab.marlin.matrix.DenseVecMatrix
import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}


object SparseMultiply {
  def main(args: Array[String]) {
    if (args.length < 5) {
      println("usage: SparseMultiply <matrixA row length> <matrixA column length> <matrixB column length>" +
        " <density> <mode>")
      System.exit(1)
    }
    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt
    val density = args(3).toDouble
    val mode = args(4).toInt
    println(args.mkString(", "))
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val mat = MTUtils.randomSpaVecMatrix(sc, rowA, colA, density)
    val mat2 = MTUtils.randomSpaVecMatrix(sc, rowB, colB, density)
    if (mode == 1) {
      val t0 = System.currentTimeMillis()
      val result = mat.multiplySparse(mat2)
      println(s"result entries count: ${result.entries.count()}")
      println(s"sparse-multiply used time: ${System.currentTimeMillis() - t0}")
    }else {
      val rowsA = mat.rows.mapValues(sv => sv.toDenseVector)
      val rowsB = mat2.rows.mapValues(sv => sv.toDenseVector)
      val denA = new DenseVecMatrix(rowsA, rowA, colA)
      val denB = new DenseVecMatrix(rowsA, rowB, colB)
      val t0 = System.currentTimeMillis()
      val result = denA.multiply(denB, (6, 6, 6))
      println(s"result blocks count: ${result.getBlocks.count()}")
      println(s"dense-multiply used time: ${System.currentTimeMillis() - t0}")
    }
    sc.stop()
  }
}
