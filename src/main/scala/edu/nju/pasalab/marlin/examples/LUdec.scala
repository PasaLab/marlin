package edu.nju.pasalab.marlin.examples

import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Test LU decompose method, this method is still to be updated
 * Only in spark-shell or local mode, you can see the print result.
 */
object LUdec {
  def main(args: Array[String]) {
    if (args.length < 2){
      System.err.println("arguments wrong, the arguments should be " +
        "<input file path> <load partitions>  " )
      System.exit(-1)
    }
    val conf = new SparkConf()
    conf.set("spark.storage.memoryFraction", "0.3")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.storage.blockManagerTimeoutIntervalMs", "80000")
    conf.set("spark.shuffle.file.buffer.kb", "200")
    conf.set("spark.akka.threads", "8")
    conf.set("spark.local.dir", "/data/spark_dir")
    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    conf.set("spark.shuffle.consolidateFiles", "true")
    val sc = new SparkContext(conf)
//    sc.setCheckpointDir("hdfs://master:54300/checkpoints")
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

    val ma = MTUtils.loadMatrixFile(sc, args(0), args(1).toInt)
    println("start LU decompose")
    val result = ma.luDecompose()
//    println("start save the L into file system")
//    result._1.saveToFileSystem("hdfs://master:54300/luDecomposeL")
    println("upper matrix rows: "+ result._2.rows.count())
    println("upper matrix first row size: "+ result._2.rows.first._2.size)
//    val l = MTUtils.loadMatrixFiles(sc, "hdfs://master:54300/luDecomposeL")
//    val u = MTUtils.loadMatrixFiles(sc, "hdfs://master:54300/luDecomposeU")
//    val mat = l.multiply(u,2)
//    mat.saveToFileSystem(args(1))
    sc.stop()
  }
}
