package edu.nju.pasalab.marlin.examples

import breeze.linalg.{DenseVector => BDV}
import edu.nju.pasalab.marlin.matrix.{DenseVecMatrix, BlockMatrix, DistributedVector}
import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkConf, SparkContext}

// implement a neural network with batch training
object NeuralNetwork {

  def loadMNISTImages(sc: SparkContext, input: String, vectorLen: Int, blkNum: Int): BlockMatrix = {
    val values = sc.textFile(input, blkNum)
    values.cache()
    val partitionInfo = values.mapPartitionsWithIndex {
      (id, iter) => Iterator.single[(Int, Int)](id, iter.size)
    }.collect().toMap
    val partitionIndex = new Array[(Int, Int)](partitionInfo.size)
    var count = 0
    for (i <- 0 until partitionInfo.size) {
      partitionIndex(i) = (count, partitionInfo.get(i).get + count - 1)
      count += partitionInfo.get(i).get
    }

    // generate rows in the format of (index, breeze vector)
    val rows = values.mapPartitionsWithIndex { (id, iter) =>
      val (start, end) = partitionIndex(id)
      val indices = start.toLong to end.toLong
      indices.toIterator.zip(iter.map{ line =>
        val items = line.split("\\s+")
        val indicesAndValues = items.tail.map { item =>
          val indexAndValue = item.split(":")
          // mnist index starts from 1
          val ind = indexAndValue(0).toInt - 1
          val value = indexAndValue(1).toDouble
          (ind, value)
        }
        val array = Array.ofDim[Double](vectorLen)
        for ((i, v) <- indicesAndValues) {
          array.update(i, v)
        }
        BDV(array)
      })
    }

    // the index arrange of the generated block-matrix
    val blockIndex = {
      val subVec = math.ceil(count.toDouble / blkNum.toDouble).toInt
      val vecCount = math.ceil(count.toDouble / subVec.toDouble).toInt
      val array = new Array[(Int, Int)](vecCount)
      for (i <- 0 until vecCount) {
        array(i) = (i * subVec, (i + 1) * subVec - 1)
      }
      array
    }

    val splitStatusByRow = MTUtils.splitMethod(partitionIndex, blockIndex)

    val mat = new DenseVecMatrix(rows, count.toLong, vectorLen)
    val blkMat = mat.toBlockMatrix(splitStatusByRow, blkNum)
    blkMat.blocks.cache()

    val labels = values.mapPartitionsWithIndex{(id, iter) =>
      val array = iter.map{ line =>
        line.split("\\s+")(0).toDouble
      }.toArray
      Iterator.single[(Int, BDV[Double])](id, BDV(array))
    }
    values.unpersist()
    (blkMat)
  }

  def main(args: Array[String]) {
    if (args.length < 4) {
      println("usage: NeuralNetwork <input path> <output path> <blocks num> {<layer unit num> ...}")
      System.exit(-1)
    }
    val input = args(0)
    val blkNum = args(2).toInt
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val vectorLen = 28 * 28
    val data = loadMNISTImages(sc, input, vectorLen, blkNum)
  }
}
