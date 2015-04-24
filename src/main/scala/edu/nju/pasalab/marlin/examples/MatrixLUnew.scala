package edu.nju.pasalab.marlin.examples

import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}


object MatrixLUnew {
  def main(args: Array[String]) {
    if (args.length < 1){
      System.err.println("arguments wrong, the arguments should be " +
        "<row number of the matrix> {<base matrix size>}")
      System.exit(-1)
    }
    val conf = new SparkConf()//.setAppName("fda").setMaster("local[8]")
    conf.set("marlin.lu.basesize", Some(args(1)).getOrElse("2000"))
//    conf.set("marlin.lu.basesize", "2000")
    val sc = new SparkContext(conf)
    val mat = MTUtils.randomDenVecMatrix(sc, args(0).toInt, args(0).toInt)
//    val mat = MTUtils.randomDenVecMatrix(sc, 4000, 4000)
    mat.rows.count()
//    println(s"start original LU decompose")
//    val result1 = mat.blockLUDecompose("dist")
//    println(s"original result rows count: ${result1._1.rows.count()}")
    println(s"start new LU decompose")
    val result2 = mat.blockLUDecomposeNew("dist")
    println(s"new result blocks count: ${result2._1.blocks.count()}")
    println(s"pArray: ${result2._2.take(10).mkString(", ")}")
  }
}
