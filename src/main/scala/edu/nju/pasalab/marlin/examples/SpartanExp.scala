package edu.nju.pasalab.marlin.examples

import edu.nju.pasalab.marlin.matrix.BlockMatrix
import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{HashPartitioner, SparkContext, SparkConf}

/**
  * Created by Cloud on 2016-03-09.
  */
object SpartanExp {
  def main(args: Array[String]) {
    if (args.length < 6) {
      println("usage: SpartanExp <matrixA row length> <matrixA column length> <matrixB column length>" +
        " <m> <k> <n>")
      System.exit(1)
    }
    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt
    val m = args(3).toInt
    val k = args(3).toInt
    val n = args(3).toInt
    val conf = new SparkConf()
    val sc = new SparkContext(conf)

    val p = new HashPartitioner(m*k)

    val X4 = MTUtils.randomBlockMatrix(sc, rowA, colA, m, k)
    val X5 = MTUtils.randomBlockMatrix(sc, rowA, colA, m, k, partitioner = Some(p))

    val Y4 = MTUtils.randomBlockMatrix(sc, rowB, colB, k, n)
    val Y5 = MTUtils.randomBlockMatrix(sc, rowB, colB, k, n, partitioner = Some(p))

    X4.add(Y4).elementsCount()
    X5.add(Y5).elementsCount()

    val result = X4.add(Y4).subtract(X4.multiply(Y4))
    val result2 = X4.multiply(Y4).subtract(X4.add(Y4))
    result2.elementsCount()
    result.elementsCount()
    X5.add(Y5).asInstanceOf[BlockMatrix].getBlocks.take(1)
    sc.stop()

  }

}
