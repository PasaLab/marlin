package edu.nju.pasalab.marlin.examples

import java.util.Random

import edu.nju.pasalab.marlin.matrix.DenseVecMatrix
import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}

import breeze.linalg.CSCMatrix


import org.apache.spark.mllib.linalg.{DenseMatrix, Matrices, SparseMatrix}


object SparseMultiply {
  def main(args: Array[String]) {
    if (args.length < 6) {
      println("usage: SparseMultiply <matrixA row length> <matrixA column length> <matrixB column length>" +
        " <density1> <density2> <mode>")
      println("mode1: sparse multiply")
      println("mode2: original dense multiply")
      println("mode3: local spark sparse matrices multiply")
      System.exit(1)
    }
    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt
    val density1 = args(3).toDouble
    val density2 = args(4).toDouble
    val mode = args(5).toInt
    println(args.mkString(", "))
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val mat = MTUtils.randomSpaVecMatrix(sc, rowA, colA, density1)
    val mat2 = MTUtils.randomSpaVecMatrix(sc, rowB, colB, density2)
    if (mode == 1) {
      val t0 = System.currentTimeMillis()
      val result = mat.multiplySparse(mat2)
      println(s"result entries count: ${result.entries.count()}")
      println(s"sparse-multiply used time: ${System.currentTimeMillis() - t0}")
    }else if(mode ==2){
      val rowsA = mat.rows.mapValues(sv => sv.toDenseVector)
      val rowsB = mat2.rows.mapValues(sv => sv.toDenseVector)
      val denA = new DenseVecMatrix(rowsA, rowA, colA)
      val denB = new DenseVecMatrix(rowsA, rowB, colB)
      val t0 = System.currentTimeMillis()
      val result = denA.multiply(denB, (6, 6, 6))
      println(s"result blocks count: ${result.getBlocks.count()}")
      println(s"dense-multiply used time: ${System.currentTimeMillis() - t0}")
    }else {
      val rnd = new Random()
      val matA: SparseMatrix = Matrices.sprand(rowA, colA, density1, rnd).asInstanceOf[SparseMatrix]
      val matB =  Matrices.rand(rowB, colB, rnd).asInstanceOf[DenseMatrix]
      val t0 = System.currentTimeMillis()
      val res = matA.multiply(matB)
      println(s"local sparse multiply dense used time: ${System.currentTimeMillis() - t0}")
    }
    sc.stop()
  }
}
