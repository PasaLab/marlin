package edu.nju.pasalab.marlin.examples

import com.esotericsoftware.kryo.Kryo
import edu.nju.pasalab.marlin.matrix.{BlockID, DistributedVector}
import edu.nju.pasalab.marlin.utils.MTUtils

import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM}
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkContext, SparkConf}


object BLAS2 {
  def main(args: Array[String]) {
    if (args.length < 5){
      println("usage: BLAS2 <matrix row length> <matrix column length> <split m> <split k> <whether has collect case 1>")
      println("for example: BLAS2 10000 10000 2 2 local")
      System.exit(1)
    }
    val conf = new SparkConf()
    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.BLAS2Registrator")
    val sc = new SparkContext(conf)
    val rowLen = args(0).toInt
    val colLen = args(1).toInt
    val m = args(2).toInt
    val k = args(3).toInt
    val matrix = MTUtils.randomDenVecMatrix(sc, rowLen, colLen)
    matrix.rows.count()
    val vector = BDV.rand[Double](colLen)
    println(s"arguments: ${args.mkString(" ")}")
    println("======================================")
    println(s"local vector length: ${vector.length}")
    var t0 = System.currentTimeMillis()
    println("case (a).2 broadcast the vector out ")
    t0 = System.currentTimeMillis()
    val result2 = matrix.multiply(vector, m)
    println(s"the split num of the result2: ${result2.splitNum}")
    println(s"case (a).2 used time ${(System.currentTimeMillis() - t0)} milliseconds")
    println("======================================")

    println("case (a).3 split matrix along two dimensions")
    val distVector = DistributedVector.fromVector(sc, vector, k)
    distVector.getVectors.count()
    t0 = System.currentTimeMillis()
    val result3 = matrix.multiplySpark(distVector, (m, k))
    println(s"the count of the result3: ${result3.vectors.count()}")
    println(s"case (a).3 used time ${(System.currentTimeMillis() - t0)} milliseconds")
    println("======================================")

//    println("case (a).4 split matrix along the column")
//    t0 = System.currentTimeMillis()
//    val result4 = matrix.multiplySpark(distVector, (1, k))
//    println(s"the split num of the result4: ${result4.vectors.count()}")
//    println(s"case (a).4 used time ${(System.currentTimeMillis() - t0)} milliseconds")
//    println("======================================")
//
//    if (args(4).equals("local")) {
//      println("case (a).1 collect the matrix to local ")
//      try {
//        val result1 = matrix.toBreeze() * vector
//        println(s"top5 element of the result1: ${result1.slice(0, 5)}")
//        println(s"case (a).1 used time ${(System.currentTimeMillis() - t0)} milliseconds")
//        println("======================================")
//      } catch {
//        case e: OutOfMemoryError => {
//          println(e); sc.stop(); System.exit(-1)
//        }
//        case e: Error => {
//          println(e); sc.stop(); System.exit(-1)
//        }
//        case e: Exception => {
//          println(e); sc.stop(); System.exit(-1)
//        }
//      }
//    }

    sc.stop()
  }

}


class BLAS2Registrator extends KryoRegistrator{
  override def registerClasses(kryo: Kryo){
    kryo.register(classOf[BlockID])
    kryo.register(classOf[BDM[Double]])
    kryo.register(classOf[BDV[Double]])
  }
}
