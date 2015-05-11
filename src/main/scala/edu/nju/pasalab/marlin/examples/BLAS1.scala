package edu.nju.pasalab.marlin.examples

import breeze.linalg.{DenseMatrix => BDM}
import com.esotericsoftware.kryo.Kryo
import edu.nju.pasalab.marlin.matrix.BlockID
import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkContext, SparkConf}


object BLAS1 {
  def main(args: Array[String]) {
    val mode = args(0)
    require(mode.equals("local") || mode.equals("dist"), s"the computing mode should either be 'local' or 'dist'")
    if (args.length < 3){
      println("usage: BLAS1 local <vector length> <split num>")
      println("or")
      println("usage: BLAS1 dist <vector length> <split num>")
      System.exit(1)
    }
    val conf = new SparkConf()
    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.BLAS1Registrator")
    val sc = new SparkContext(conf)
    mode match {
      case "local" => println("multiply the two distributed vectors in local environment")
      case "dist" => println("multiply the two distributed vectors in distributed environment")
    }
    val vecLength = args(1).toInt
    val splitNum = args(2).toInt
    val vector1 = MTUtils.randomDistVector(sc, vecLength, splitNum)
    val vector2 = MTUtils.randomDistVector(sc, vecLength, splitNum)
    val t0 = System.currentTimeMillis()
    val result = vector1.transpose().multiply(vector2, mode)
    println(s"the vector multiply result: ${result.left.get}, in mode $mode")
    println(s"time used: ${(System.currentTimeMillis() - t0)} milliseconds, in mode $mode")
    sc.stop()
  }
}

class BLAS1Registrator extends KryoRegistrator{
  override def registerClasses(kryo: Kryo){
    kryo.register(classOf[BlockID])
    kryo.register(classOf[BDM[Double]])
  }
}
