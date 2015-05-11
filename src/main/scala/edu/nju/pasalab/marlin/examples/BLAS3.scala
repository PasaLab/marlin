package edu.nju.pasalab.marlin.examples

import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}


object BLAS3 {
  def main(args: Array[String]) {
    if (args.length < 3){
      println("usage: BLAS3 <matrixA row length> <matrixA column length> <matrixB column length>")
      println("for example: BLAS3 10000 10000 10000 ")
      System.exit(1)
    }
    val conf = new SparkConf()
    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.BLAS2Registrator")
    val sc = new SparkContext(conf)
    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt
    val matrixA = MTUtils.randomDenVecMatrix(sc, rowA, colA)
    val matrixB = MTUtils.randomDenVecMatrix(sc, rowB, colB)
    matrixA.rows.cache()
    matrixB.rows.cache()
    matrixA.rows.count()
    matrixB.rows.count()
    for (m <- 4 to 7) {
      for (k <- 4 to 7){
        for (n <- 4 to 7){
          println(s"split mode: ($m , $k, $n); matrixA: $rowA by $colA ; matrixB: $rowB by $colB")
          val t0 = System.currentTimeMillis()
          val result = matrixA.multiplySpark(matrixB, (m, k, n))
          result.blocks.count()
          println(s"multiplication used time ${(System.currentTimeMillis() - t0)} milliseconds")
          println("===========================================================================")
          Thread.sleep(10000)
        }
      }
    }
    sc.stop()

  }
}
