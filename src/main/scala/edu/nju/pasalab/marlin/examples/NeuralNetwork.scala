package edu.nju.pasalab.marlin.examples

import java.io.File

import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, csvwrite}
import breeze.numerics.sigmoid
import breeze.stats.distributions.{Uniform, Gaussian}

import edu.nju.pasalab.marlin.matrix._
import edu.nju.pasalab.marlin.rdd.MatrixMultPartitioner
import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partitioner, Logging, SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.HashSet

// implement a neural network with batch training
object NeuralNetwork extends Logging{

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
                      blkNum: Int): (BlockMatrix, RDD[(BlockID, BDV[Int])]) = {
    val t0 = System.currentTimeMillis()
    val values = sc.textFile(input, blkNum)
    values.cache()
    val partitionInfo = values.mapPartitionsWithIndex(
      (id, iter) => Iterator.single[(Int, Int)](id, iter.size)
      , preservesPartitioning = true).collect().toMap
    val partitionIndex = new Array[(Int, Int)](partitionInfo.size)
    var count = 0
    for (i <- 0 until partitionInfo.size) {
      partitionIndex(i) = (count, partitionInfo.get(i).get + count - 1)
      count += partitionInfo.get(i).get
    }

    // generate rows in the format of (index, breeze vector)
    val rows = values.mapPartitionsWithIndex((id, iter) => {
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
      }) }, preservesPartitioning = true)


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

    val labels = values.mapPartitionsWithIndex(
      (id, iter) => {
      val array = iter.map{ line =>
        line.split("\\s+")(0).toInt
      }.toArray
      Iterator.single[(Int, BDV[Int])](id, BDV(array))}
      , preservesPartitioning = true)
    val disVector = new DistributedIntVector(labels)
    val distributedVector = disVector.toDisVector(splitStatusByRow, blkNum)
      .getVectors.map{case(vectorId, vec) => (BlockID(vectorId, 0), vec)}
    values.unpersist()
    println(s"load mnist image used time: ${System.currentTimeMillis() - t0} millis")
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
    val set = new mutable.HashSet[Int]()
    for (i <- 0 until executors) {
      while (set.size < (i + 1) * selectedEachExecutor) {
        set.+=(uni.sample().toInt + i * executors)
      }
    }
    set
  }



  def dSigmoid(x: Double): Double = {
    (1 - sigmoid(x)) * sigmoid(x)
  }

  /**
   * compute the output layer error for the next back propagation
   * @param output the output result
   * @param labels the lables
   * @return
   */
  def computeOutputError(
        output: RDD[(BlockID, BDM[Double])],
        labels: RDD[(BlockID, BDV[Int])]): RDD[(BlockID, BDM[Double])] = {
    output.join(labels).mapValues{case(blk, vec) =>
        println(s"compute output error, vec activesize: ${vec.activeSize}")
        for(i <- 0 until vec.length) {
          blk(i, vec(i)) -= 1.0
        }
        blk
    }
  }

  /**
   * after get the other layer's delta, then compute the error
   * NOTE:: the weight matirx should be the tranposed view one
   * @param outputDelta
   * @param weight
   * @return
   */
  def computeLayerError(
        outputDelta: RDD[(BlockID, BDM[Double])],
        weight: BDM[Double]): RDD[(BlockID, BDM[Double])] = {
    outputDelta.mapPartitions(iter =>
      iter.map{case(blkId, blk) => (blkId, blk * weight)}
    , preservesPartitioning = true)
  }

  /**
   * compute the delta of each weight layer
   * @param input
   * @param error
   * @return
   */
  def computeDelta(input: RDD[(BlockID, BDM[Double])],
                   error: RDD[(BlockID, BDM[Double])]): RDD[(BlockID, BDM[Double])] = {
    val dActivation = input.mapPartitions(iter =>
      iter.map{case(blkId, blk) => (blkId, blk.map(x => dSigmoid(x)))}
    , preservesPartitioning = true)
    dActivation.join(error).map{ case(blkId, (blk1, blk2)) =>
      blk1 :*=  blk2
      println(s"compute delta, blk1 dimension: ${blk1.rows} by ${blk1.cols}")
      (blkId, blk1)
    }
  }

  /**
   * compute the updated weight of the weight matrix
   * @param input
   * @param delta
   * @param learningRate
   * @return
   */
  def computeWeightUpd(
        input: RDD[(BlockID, BDM[Double])],
        delta: RDD[(BlockID, BDM[Double])],
        learningRate: Double): BDM[Double] = {
    val inputTranspose = input.mapValues(blk => blk.t)
    inputTranspose.join(delta).mapPartitions(iter =>
      iter.map { case (blkId, (inputT, d)) =>
        println(s"inputT dimension: ${inputT.rows} by ${inputT.cols}")
        val tmp = (inputT * d).asInstanceOf[BDM[Double]]
        println(s"tmp result matrix 1st row: ${tmp(0, ::)}")
        tmp * learningRate
      }
    ).reduce(_ + _)
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
    val selectedEachExecutor = args(5).toInt
    val layerNum = args(6).toInt
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val vectorLen = 28 * 28
    // when initializing the weight matrix, the elements should be close to zero
    val dis = new Gaussian(0, 0.2)
    val hiddenWeight = BDM.rand[Double](vectorLen, layerNum, dis)
    val outputWeight = BDM.rand[Double](layerNum, 10, dis)
    println(s"weight matrix updated")
    val (oriData, oriLabels) = loadMNISTImages(sc, input, vectorLen, blkNum)
    val partitioner = new NeuralNetworkPartitioner(blkNum)
    val data = oriData.getBlocks.partitionBy(partitioner)
    data.cache()
    val labels = oriLabels.partitionBy(partitioner)
    labels.cache()
//    data.getBlocks.cache()
//    labels.cache()
    println(s"data partitioner: ${data.partitioner}")
    println(s"labels partitioner: ${labels.partitioner}")
    for (i <- 0 until iterations) {
      val t0 = System.currentTimeMillis()
      // here we assume it has 16 * 24 = 384 blocks in total,
      // every time sample 32 blocks out, each executor has 2 block
      val set = genRandomBlocks(16, 24, selectedEachExecutor)

      /** Propagate through the network by mini-batch SGD **/
//      val tp = System.currentTimeMillis()
      val inputData = data.filter{case(blkId, _) => set.contains(blkId.row)}.cache()
//      println(s"inputData rdd count: ${inputData.count()}")
      println(s"inputData partitioner: ${inputData.partitioner}")
//      println(s"inputData rdd count: ${inputData.count()}")
      val hiddenLayerInput = inputData.mapPartitions(
        iter => iter.map{case(blkId, block) =>
          (blkId, (block * hiddenWeight).asInstanceOf[BDM[Double]])}
      , preservesPartitioning = true).cache()
      println(s"hiddenLayerInput partitioner: ${hiddenLayerInput.partitioner}")
      val hiddenLayerOut = hiddenLayerInput.mapValues(_.map(x => sigmoid(x))).cache()
      println(s"hiddenLayerOut partitioner: ${hiddenLayerOut.partitioner}")
      val outputlayerInput = hiddenLayerOut.mapPartitions(
        iter => iter.map{case(blkId, block) =>
          println(s"get the input of the outpu layer, block size: ${block.rows} by ${block.cols}")
          (blkId, (block * outputWeight).asInstanceOf[BDM[Double]])}
        , preservesPartitioning = true).cache()
      println(s"outputlayerInput partitioner: ${outputlayerInput.partitioner}")
      val outputLayerOut = outputlayerInput.mapValues(_.map(x => sigmoid(x))).cache()
      println(s"outputLayerOut count: ${outputLayerOut.count()}")
      println(s"outputLayerOut partitioner: ${outputLayerOut.partitioner}")
//      println(s"in iteration $i, " +
//        s"Propagate through the network used time: ${System.currentTimeMillis() - tp} millis")


      /** Back Propagate the errors **/
//      val tb = System.currentTimeMillis()
      val selectedLabels = labels.filter{case(blockId, _) => set.contains(blockId.row)}
//      println(s"selectedLabels rdd count: ${selectedLabels.count()}")
      // update the output layer
      val outputError = //computeOutputError(outputLayerOut, selectedLabels)
      outputLayerOut.join(selectedLabels).mapValues{case(blk, vec) =>
        println(s"compute output error, vec activesize: ${vec.activeSize}")
        for(i <- 0 until vec.length) {
          blk(i, vec(i)) -= 1.0
        }
        blk
      }
      println(s"output error count: ${outputError.count()}")
      val outputDelta = computeDelta(outputlayerInput, outputError)
      println(s"output delta count: ${outputDelta.count()}")

      // update the hidden layer
      val hiddenError = computeLayerError(outputDelta, outputWeight.t)
      println(s"hidden error count: ${hiddenError.count()}")
      val hiddenDelta = computeDelta(hiddenLayerInput, hiddenError)
      println(s"hidden delta count: ${hiddenDelta.count()}")
      //      println(s"in iteration $i, " +
//        s"back propagate the errors used time: ${System.currentTimeMillis() - tp} millis")
      /** update the weights **/
//      val hiddenOutTranspose = hiddenLayerOut.mapValues(blk => blk.t)
//      val outWeightUpd = hiddenOutTranspose.join(outputDelta).mapPartitions(iter =>
//        iter.map { case (blkId, (inputT, delta)) =>
//          ((inputT * delta).asInstanceOf[BDM[Double]] * learningRate).asInstanceOf[BDM[Double]]}
//      ).reduce(_ + _)
      val outWeightUpd = computeWeightUpd(hiddenLayerOut, outputDelta, learningRate)
      println(s"outWeightUpd: \n $outWeightUpd")
      outputWeight -= outWeightUpd

      val hiddenWeightUpd = computeWeightUpd(inputData, hiddenDelta, learningRate)
      hiddenWeight -= hiddenWeightUpd
//      println(s"in iteration $i, " +
//        s"update the weights used time: ${System.currentTimeMillis() - tb} millis")

      hiddenLayerInput.unpersist()
      hiddenLayerOut.unpersist()
      outputlayerInput.unpersist()
      inputData.unpersist()
      outputLayerOut.unpersist()

      println(s"in iteration $i, used time: ${System.currentTimeMillis() - t0} millis")
    }

    csvwrite(new File("hiddenWeight"), hiddenWeight)
    csvwrite(new File("outputWeight"), outputWeight)

    sc.stop()
  }
}

private[marlin] class NeuralNetworkPartitioner(val blockNum: Int)
  extends Partitioner with Logging{

  override def numPartitions: Int = blockNum

  override def getPartition(key: Any): Int = {
    key match {
      case (blockId: BlockID) =>
        blockId.row
      case (vectorId: Int) => vectorId
      case _ =>
        throw new IllegalArgumentException(s"Unrecognized key: $key")
    }
  }

  override def equals(obj: Any): Boolean = {
    obj match {
      case p: NeuralNetworkPartitioner =>
        this.blockNum == p.blockNum
      case _ =>
        false
    }
  }
}
