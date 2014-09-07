package edu.nju.pasalab.examples

import org.apache.spark.{SparkContext, SparkConf}

import edu.nju.pasalab.sparkmatrix.MTUtils

/**
 * Two large matrices multiply
 */
object MatrixMultiply {

  /**
   * Function :Two large matrices multiply
   *
   * @param args args(0):<input file path A> args(1)<input file path B> args(2)<output file path> args(3)<block num>
   */
   def main (args: Array[String]) {
    if (args.length != 4){
      System.err.println("arguments wrong, the arguments should be " +
        "<input file path A> <input file path B> <output file path> <block num>")
      System.exit(-1)
    }
    val conf = new SparkConf().setAppName("FileMatrixMultiply")
     conf.set("spark.storage.memoryFraction", "0.5")
     val sc = new SparkContext(conf)
     val ma = MTUtils.loadMatrixFile(sc,args(0))
     val mb = MTUtils.loadMatrixFile(sc,args(1))
     val result =  ma.multiply(mb, args(3).toInt )
     result.saveToFileSystem(args(2))
     sc.stop()

  }

}
