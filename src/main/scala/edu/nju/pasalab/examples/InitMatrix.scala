package edu.nju.pasalab.examples

import org.apache.spark.{SparkContext, SparkConf}

import edu.nju.pasalab.sparkmatrix.{MTUtils, IndexMatrix}

/**
 * Test method matrixToArray in [[edu.nju.pasalab.sparkmatrix.MTUtils]]
 * Only in spark-shell or local mode, you can see the print result
 */
object InitMatrix {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test init IndexMatrix")
    val sc = new SparkContext(conf)
    val array = Array.ofDim[Double](1000,1000)
    for (i <- 0 until 1000){
      val a = Array.ofDim[Double](1000)
      for ( j <- 0 until 1000){
        a(j) = 1000 - j
       }
      array(i) = a
    }
    val mat = new IndexMatrix(sc, array)
    println("row 11 to 12")
    println( mat.sliceByRow(11,12).rows.take(1).apply(0)  )
    println("row 997 to 998")
    println(mat.sliceByRow(997,998).rows.take(1).apply(0)  )


    println("-------------IndexMatrix to Array")
    val arr = MTUtils.matrixToArray(mat)
    println("arr 0 0 "+arr(0)(0))
    println("arr 5 0 "+arr(5)(0))
    println("arr 8 8 "+arr(8)(8))
    sc.stop()
  }

}
