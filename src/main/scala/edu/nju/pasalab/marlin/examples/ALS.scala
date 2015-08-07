package edu.nju.pasalab.marlin.examples

import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}

/**
 * This is an example to show how to run ALS
 */
object ALS {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: ALS <input> <rank> <iteration> <lambda> {<numUserBlock> <numProductBlock>}")
      System.exit(1)
    }
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val inputData = args(0)
    val rank = args(1).toInt
    val iterations = args(2).toInt
    val lambda = args(3).toDouble
    val numUserBlock = if (args.length > 4) args(4).toInt else 10
    val numProductBlock = if (args.length > 5) args(5).toInt else 10
    val ratingMatrix = MTUtils.loadCoordinateMatrix(sc, inputData)
    val (userFactor, productFactor) = ratingMatrix.ALS(rank, iterations, lambda, numUserBlock, numProductBlock)
    userFactor.getRows.count()
    productFactor.getRows.count()
  }

}
