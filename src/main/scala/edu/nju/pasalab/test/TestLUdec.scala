package edu.nju.pasalab.test

import edu.nju.pasalab.sparkmatrix.{MTUtils, IndexMatrix, IndexRow, Vectors}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by hadoop on 8/29/14.
 */
object TestLUdec {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("test lu decompose")
    val sc = new SparkContext(conf)
    sc.setCheckpointDir("hdfs://master:54300/checkpoints")
//    val data = Seq(
//      (0L, Vectors.dense(1.0,2.0,3.0)),
//      (1L, Vectors.dense(4.0,5.0,6.0)),
//      (2L, Vectors.dense(7.0,8.0,0.0))
//    ).map(t => IndexRow(t._1 , t._2))
//
//    val mat = new IndexMatrix( sc.parallelize(data,2) )

//    println("start LU decompose, and println them out")
//    mat.rows.foreach( t => println(t.toString))
//    mat.luDecompose._2.rows.foreach(t => println(t.toString)  )
//    mat.saveToFileSystem("hdfs://master:54300/luori")
//    val result = mat.luDecompose()
//    result._1.saveToFileSystem("hdfs://master:54300/L")
//    result._2.saveToFileSystem("hdfs://master:54300/U")
//    mat.saveToFileSystem("hdfs://master:54300/luori2")
//    val result2 = mat.luDecompose()
//    result2._1.saveToFileSystem("hdfs://master:54300/L2")

    val ma = MTUtils.loadMatrixFile(sc,args(0), 10)
    println("start LU decompose")
    val result = ma.luDecompose()
    println("start save the L into file system")
//    result._1.saveToFileSystem("hdfs://master:54300/luDecomposeL")
    println("start save the U into file system")
    result._2.saveToFileSystem(args(1))
//    val l = MTUtils.loadMatrixFiles(sc, "hdfs://master:54300/luDecomposeL")
//    val u = MTUtils.loadMatrixFiles(sc, "hdfs://master:54300/luDecomposeU")
//    val mat = l.multiply(u,2)
//    mat.saveToFileSystem(args(1))
    sc.stop()
  }
}
