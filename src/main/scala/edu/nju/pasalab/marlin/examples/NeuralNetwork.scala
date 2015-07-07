package edu.nju.pasalab.marlin.examples

import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM}
import breeze.stats.distributions.{Uniform, Gaussian}
import edu.nju.pasalab.marlin.matrix.{BlockID, DenseVecMatrix, BlockMatrix, DistributedVector}
import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.HashSet

// implement a neural network with batch training
object NeuralNetwork {

  /**
   * load mnist data and the input store as block-matrix, while the labels
   * store as (blockId, vector) instead of distributed vector
   * for the easy usage of next join step
   * @param sc
   * @param input
   * @param vectorLen
   * @param blkNum
   * @return
   */
  def loadMNISTImages(sc: SparkContext, input: String, vectorLen: Int,
                      blkNum: Int): (BlockMatrix, RDD[(BlockID, BDV[Double])]) = {

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
    val disVector = new DistributedVector(labels)
    val distributedVector = disVector.toDisVector(splitStatusByRow, blkNum)
      .getVectors.map{case(vectorId, vec) => (BlockID(vectorId, 0), vec)}
    values.unpersist()
    (blkMat, distributedVector)
  }

  /**
   * generate the selected random blocks
   * @param executors
   * @param blockEachExecutor
   * @param selectedEachExecutor
   * @return
   */
  def genRandomBlocks(executors: Int, blockEachExecutor: Int,
                      selectedEachExecutor: Int): HashSet[Int] = {
    require(selectedEachExecutor <= blockEachExecutor, s"the sampled blocks " +
      s"on each executor should be less than the total blocks on each executor")
    val uni = new Uniform(0, blockEachExecutor - 1)
    val set = new HashSet[Int]()
    for (i <- 0 until executors) {
      while (set.size < (i + 1) * selectedEachExecutor) {
        set.+(uni.sample().toInt + i * executors)
      }
    }
    set
  }

  def sigmod(x: Double): Double = {
    1/(1 + math.exp(-x))
  }

  def dSigmod(x: Double): Double = {
    (1 - sigmod(x)) * sigmod(x)
  }

  def computeOutputError(
        output: RDD[(BlockID, BDM[Double])],
        labels: RDD[(BlockID, BDV[Double])]): RDD[(BlockID, BDM[Double])] = {
//    output.cogroup()
    ???
  }

  def main(args: Array[String]) {
    if (args.length < 7) {
      println("usage: NeuralNetwork <input path> <output path> <iterations>  " +
        "<learningRate> <blocks num> " +
        "<block on each executor> {<layer unit num> ...}")
      System.exit(-1)
    }
    val input = args(0)
    val iterations = args(2).toInt
    val learningRate = args(3).toDouble
    val blkNum = args(4).toInt
    val blockEachExecutor = args(5).toInt
    val layerNum = args(6).toInt
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val vectorLen = 28 * 28
    val (data, labels) = loadMNISTImages(sc, input, vectorLen, blkNum)
    data.getBlocks.cache()
    labels.cache()
    // when initializing the weight matrix, the elements should be close to zero
    val dis = new Gaussian(0, 0.2)
    val hiddenWeight = BDM.rand[Double](vectorLen, layerNum, dis)
    val outputWeight = BDM.rand[Double](layerNum, 10, dis)

    for (i <- iterations) {
      // here we assume it has 16 * 24 = 384 blocks in total,
      // every time sample 32 blocks out, each executor has 2 block
      val set = genRandomBlocks(16, 24, blockEachExecutor)

      // Propagate through the network by mini-batch SGD
      val hiddenLayerInput = data.blocks.filter{case(blkId, _) => set.contains(blkId.row)}
        .mapPartitions{
        iter => iter.map{case(blkId, block) =>
          (blkId, (block * hiddenWeight).asInstanceOf[BDM[Double]])}
      }.cache()
      val hiddenLayerOut = hiddenLayerInput.mapValues(_.map(x => sigmod(x))).cache()
      val outputlayerInput = hiddenLayerOut.mapPartitions{
        iter => iter.map{case(blkId, block) =>
          (blkId, (block * outputWeight).asInstanceOf[BDM[Double]])}
      }.cache()
      val outputLayerOut = outputlayerInput.mapValues(_.map(x => sigmod(x))).cache()


      // Back Propagate the errors
      labels.filter{case(blockId, _) => set.contains(blockId.row)}
//      val outputDelta =
    }

  }
}
