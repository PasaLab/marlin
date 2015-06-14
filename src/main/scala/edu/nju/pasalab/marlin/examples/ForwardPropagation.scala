package edu.nju.pasalab.marlin.examples

import java.util.Calendar

import edu.nju.pasalab.marlin.utils.MTUtils

import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.{SparkContext, SparkConf}

/**
 * This Forward Propagation example read data from
 * mnist8m [[http://www.csie.ntu.edu.tw/~cjlin/libsvmtools/datasets/multiclass.html]], here we
 * use 3 layer neural network
 */
object ForwardPropagation {
  def main(args: Array[String]) {
    if (args.length < 1){
      println("usage: ForwardPropagation <input path> <parallelism> ")
      System.exit(-1)
    }
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val vectorLen = 28 * 28
    val input = args(0)
    val minPartitions = if (args.length > 1) {args(1).toInt} else {sc.defaultParallelism}
    val inputDataMatrix = MTUtils.loadSVMDenVecMatrix(sc, input, vectorLen, minPartitions)
    val lay1 = 200
    val lay2 = 100
    val lay3 = 50
    val labels = 10
    val layer1 = BDM.rand[Double](vectorLen, lay1)
    val layer2 = BDM.rand[Double](lay1, lay2)
    val layer3 = BDM.rand[Double](lay2, lay3)
    val lastLayer = BDM.rand[Double](lay3, labels)
    println(s"start to run DNN, at ${Calendar.getInstance().getTime}")
    val t0 = System.currentTimeMillis()
    val result = inputDataMatrix.multiplyBroadcast(layer1).multiplyBroadcast(layer2)
      .multiplyBroadcast(layer3).multiplyBroadcast(lastLayer)
    result.rows.count()
    println(s"Forward propagation in DNN used: ${System.currentTimeMillis() - t0} millis")
    println(s"the first row of the result: ${result.rows.first()._2.toArray.mkString(",")}")
    sc.stop()
  }
}
