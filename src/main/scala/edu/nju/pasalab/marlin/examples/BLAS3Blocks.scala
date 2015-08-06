package edu.nju.pasalab.marlin.examples

import java.util.Calendar

import edu.nju.pasalab.marlin.utils.MTUtils

import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.{SparkContext, SparkConf}

object BLAS3Blocks {
  def main(args: Array[String]) {
    if (args.length < 4) {
      println("usage: BLAS3Blocks <matrixA row length> <matrixA column length> <matrixB column length> " +
        "<mode> {original <aRowBlk> <aColBlk> <bRowBlk> <bColBlk>} {new <m> <k> <n>}")
      println("one of the matrix is distributed in BlockMatrix format")
      println("mode 1 means broadcast one of the matrix out, and execute multiplication")
      println("mode 2 means shuffle the row matrix to block matrix, and then execute multiplication")
      println("mode 3 means both of th matrices are BlockMatrix, need to shuffle one of them, " +
        " and then execute multiplication")
      println("for example: BLAS3Blocks 10000 10000 10000 1 5 5")
      System.exit(1)
    }
    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt
    val mode = args(3).toInt
    val conf = new SparkConf()
    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.BLAS2Registrator")
    val sc = new SparkContext(conf)
    println(s"arguments: ${args.mkString(" ")}, ${Calendar.getInstance().getTime()}")
    if (mode == 1) {
      val m = args(4).toInt
      val k = args(5).toInt
      val matA = MTUtils.randomBlockMatrix(sc, rowA, colA, m, k)
      val matB = BDM.rand[Double](rowB, colB)
      val t0 = System.currentTimeMillis()
      val result = matA.multiply(matB)
      result.blocks.count()
      println(s"in mode ${mode}, used time ${System.currentTimeMillis() - t0} millis,"+
        s"${Calendar.getInstance().getTime()}")
    } else if (mode == 2) {
      val aRowBlk = args(4).toInt
      val aColBlk = args(5).toInt

      val m = args(6).toInt
      val k = args(7).toInt
      val n = args(8).toInt
      val matA = MTUtils.randomBlockMatrix(sc, rowA, colA, aRowBlk, aColBlk)
      // this step is to let the generate block matrix distribute in the cluster uniformly
      matA.blocks.count()
      val matB = MTUtils.randomDenVecMatrix(sc, rowB, colB)
      val t0 = System.currentTimeMillis()
      val blkMatA = matA.toBlockMatrix(m, k)
      val blkMatB = matB.toBlockMatrix(k, n)
      val result = blkMatA.multiplySpark(blkMatB)
      result.blocks.count()
      println(s"in mode ${mode}, used time ${System.currentTimeMillis() - t0} millis" +
        s", ${Calendar.getInstance().getTime()}")
    }else {
      val aRowBlk = args(4).toInt
      val aColBlk = args(5).toInt
      val bRowBlk = args(6).toInt
      val bColBlk = args(7).toInt

      val m = args(8).toInt
      val k = args(9).toInt
      val n = args(10).toInt

      val matA = MTUtils.randomBlockMatrix(sc, rowA, colA, aRowBlk, aColBlk)
      val matB = MTUtils.randomBlockMatrix(sc, rowB, colB, bRowBlk, bColBlk)
      matA.blocks.count()
      matB.blocks.count()
      val t0 = System.currentTimeMillis()
      val blockMatA = matA.toBlockMatrix(m, k)
      val blockMatB = matB.toBlockMatrix(k, n)
      val result = blockMatA.multiplySpark(blockMatB)
      result.blocks.count()
      println(s"in mode ${mode}, used time ${System.currentTimeMillis() - t0} millis" +
        s", ${Calendar.getInstance().getTime()}")
    }
    sc.stop()
  }
}
