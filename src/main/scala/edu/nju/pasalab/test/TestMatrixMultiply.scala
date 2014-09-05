package edu.nju.pasalab.test

import com.esotericsoftware.kryo.Kryo
import edu.nju.pasalab.sparkmatrix.{BlockID, IndexRow, IndexMatrix, MTUtils}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by PASAlab@NJU on 7/29/14.
 */
object TestMatrixMultiply {
  /**
   * Author:Yabby
   */
   def main (args: Array[String]) {

    val conf = new SparkConf().setAppName("FileMatrixMultiply V5")
     val sc = new SparkContext(conf)
     val ma = MTUtils.loadMatrixFile(sc,args(0))
     val mb = MTUtils.loadMatrixFile(sc,args(1))
     val result =  ma.multiply(mb, args(3).toInt )
     result.saveToFileSystem(args(2))
     sc.stop()

  }

}
