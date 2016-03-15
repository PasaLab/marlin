package edu.nju.pasalab.marlin.examples

import java.util.Calendar

import edu.nju.pasalab.marlin.matrix.DenseVecMatrix
import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}


object SparseMultiply {
  def main(args: Array[String]) {
    if (args.length < 5) {
      println("usage: SparseMultiply <matrixA row length> <matrixA column length> <matrixB column length>" +
        " <density> <mode>")
      println("mode 1: SparseVecMatrix multiply SparseVecMatrix with CRM algorithm")
      println("mode 2: SparseVecMatrix multiply SparseVecMatrix with sparse to dense vector")
      println("mode 3: BlockMatrix multiply BlockMatrix with sparse format")
      println("mode 4: BlockMatrix multiply BlockMatrix with dense format")
      println("mode 5: Dense BlockMatrix multiply Sparse BlockMatrix with dense-sparse format")
      println("mode 6: Dense BlockMatrix multiply Sparse BlockMatrix with dense-dense format")
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
    mode match {
      case 1 =>
        val mat = MTUtils.randomSpaVecMatrix(sc, rowA, colA, density)
        val mat2 = MTUtils.randomSpaVecMatrix(sc, rowB, colB, density)
        val t0 = System.currentTimeMillis()
        val result = mat.multiplySparse(mat2)
        println(s"result entries count: ${result.entries.count()}")
        println(s"sparse-multiply used time: ${System.currentTimeMillis() - t0}, ;${Calendar.getInstance().getTime}")
      case 2 =>
        val mat = MTUtils.randomSpaVecMatrix(sc, rowA, colA, density)
        val mat2 = MTUtils.randomSpaVecMatrix(sc, rowB, colB, density)
        val rowsA = mat.rows.mapValues(sv => sv.toDenseVector)
        val rowsB = mat2.rows.mapValues(sv => sv.toDenseVector)
        val denA = new DenseVecMatrix(rowsA, rowA, colA)
        val denB = new DenseVecMatrix(rowsB, rowB, colB)
        val t0 = System.currentTimeMillis()
        val result = denA.multiply(denB, (6, 6, 6))
        println(s"result blocks count: ${result.getBlocks.count()}")
        println(s"dense-multiply used time: ${System.currentTimeMillis() - t0};${Calendar.getInstance().getTime}")
      case 3 =>
        val matA = MTUtils.randomBlockMatrix(sc, rowA, colA, 6, 6, (true, density))
        val matB = MTUtils.randomBlockMatrix(sc, rowB, colB, 6, 6, (true, density))
        val t0 = System.currentTimeMillis()
        val result = matA.multiply(matB)
        println(s"result blocks count: ${result.getBlocks.count()}")
        println(s"BlockMatrix-BlockMatrix multiply with sparse format used time: ${System.currentTimeMillis() - t0} " +
          s";${Calendar.getInstance().getTime}")
      case 4 =>
        val matA = MTUtils.randomBlockMatrix(sc, rowA, colA, 6, 6, (true, density)).toDenseBlocks
        val matB = MTUtils.randomBlockMatrix(sc, rowB, colB, 6, 6, (true, density)).toDenseBlocks
        val t0 = System.currentTimeMillis()
        val result = matA.multiply(matB)
        println(s"result blocks count: ${result.getBlocks.count()}")
        println(s"BlockMatrix-BlockMatrix multiply with dense format used time: ${System.currentTimeMillis() - t0} " +
          s";${Calendar.getInstance().getTime}")
      case 5 =>
        val matA = MTUtils.randomBlockMatrix(sc, rowA, colA, 6, 6)
        val matB = MTUtils.randomBlockMatrix(sc, rowB, colB, 6, 6, (true, density))
        val t0 = System.currentTimeMillis()
        val result = matA.multiply(matB)
        println(s"result blocks count: ${result.getBlocks.count()}")
        println(s"Dense BlockMatrix multiply Sparse BlockMatrix with dense-sparse format used time: ${System.currentTimeMillis() - t0} " +
          s";${Calendar.getInstance().getTime}")
      case 6 =>
        val matA = MTUtils.randomBlockMatrix(sc, rowA, colA, 6, 6)
        val matB = MTUtils.randomBlockMatrix(sc, rowB, colB, 6, 6, (true, density)).toDenseBlocks
        val t0 = System.currentTimeMillis()
        val result = matA.multiply(matB)
        println(s"result blocks count: ${result.getBlocks.count()}")
        println(s"Dense BlockMatrix multiply Sparse BlockMatrix with dense-dense format used time: ${System.currentTimeMillis() - t0} " +
          s";${Calendar.getInstance().getTime}")
    }
    sc.stop()
  }
}
