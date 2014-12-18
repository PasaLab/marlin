package edu.nju.pasalab.marlin.examples

import breeze.linalg.{DenseMatrix => BDM}
import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkContext, SparkConf}

import edu.nju.pasalab.marlin.matrix.BlockID
import edu.nju.pasalab.marlin.utils.MTUtils

import edu.nju.pasalab.marlin.matrix._


/**
 * Two large matrices multiply example
 */
object SVD {

  def main(args: Array[String]) {

    val conf = new SparkConf()
    /**if the matrices are too large, you can set below properties to tune the Spark well*/
    conf.set("spark.storage.memoryFraction", "0.4")
    conf.set("spark.eventLog.enabled", "true")
    conf.set("spark.storage.blockManagerTimeoutIntervalMs", "80000")
    conf.set("spark.shuffle.file.buffer.kb", "200")
    conf.set("spark.akka.threads", "8")
//    conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
//    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.MyRegistrator")
    conf.set("spark.kryoserializer.buffer.max.mb", "100")
//    conf.set("spark.local.dir", "/data/spark_dir")
//    conf.set("spark.shuffle.consolidateFiles", "true")

    val sc = new SparkContext(conf)
    
    val row = Seq(
     (0L, Vectors.dense(2.0, 0.0, 8.0, 6.0, 0.0)),
     (1L, Vectors.dense(1.0, 6.0, 0.0, 1.0, 7.0)), 
     (2L, Vectors.dense(5.0, 0.0, 7.0, 4.0, 0.0)), 
     (3L, Vectors.dense(7.0, 0.0, 8.0, 5.0, 0.0)), 
     (4L, Vectors.dense(0.0, 10.0, 0.0, 0.0, 7.0))
     ).map(t => (t._1, t._2))
     val mat = new DenseVecMatrix(sc.parallelize(row,2))
     val svd = mat.computeSVD(3, true)
     println("U:")
     svd.U.printAll();
     println("SIGMA:")
     svd.s.toArray.foreach(println)
     println("V:")
     svd.V.toString();
     
     
     /*
    val ma = MTUtils.randomDenVecMatrix(sc, args(0).toInt, args(1).toInt)
    val mb = MTUtils.randomDenVecMatrix(sc, args(1).toInt, args(2).toInt)
    val threshold = if (args.length < 6) {
      300
    }else { args(5).toInt }
    val result = ma.multiply(mb, args(3).toInt, threshold)
    println("Result RDD counts: " + result.blocks.count())
    println("start store the result matrix in DenseVecMatrix type")
    result.saveToFileSystem(args(4))
    * */
    sc.stop()
  }
}
