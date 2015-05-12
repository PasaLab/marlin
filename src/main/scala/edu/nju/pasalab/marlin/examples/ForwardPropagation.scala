package edu.nju.pasalab.marlin.examples

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
      println("usage: ForwardPropagation <input path> <cores>")
      System.exit(-1)
    }
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val vectorLen = 28 * 28
    val inputDataMatrix = MTUtils.loadSVMDenVecMatrix(sc, args(0), vectorLen)
    val lay1 = 200
    val lay2 = 100
    val lay3 = 50
    val labels = 10
    val layer1 = BDM.rand[Double](vectorLen, lay1)
    val layer2 = BDM.rand[Double](vectorLen, lay2)
    val layer3 = BDM.rand[Double](vectorLen, lay3)
    val lastLayer = BDM.rand[Double](lay3, labels)
    val result = inputDataMatrix.oldMultiplyBroadcast(layer1, args(1).toInt).multiply(layer2)
      .multiply(layer3).multiply(lastLayer)
    result.getBlocks.count()
  }
}
