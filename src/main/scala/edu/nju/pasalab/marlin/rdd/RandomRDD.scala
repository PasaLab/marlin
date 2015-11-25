package edu.nju.pasalab.marlin.rdd

import org.apache.spark.annotation.DeveloperApi

import scala.collection.mutable.HashSet
import scala.util.Random

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import edu.nju.pasalab.marlin.matrix._
import edu.nju.pasalab.marlin.utils.{MTUtils, RandomDataGenerator}

private[marlin] class RandomRDDPartition[T](override val index: Int,
    val start: Long,
    val size: Int,
    val generator: RandomDataGenerator[T],
    val seed: Long) extends Partition {

  require(size >= 0, "Non-negative partition size required.")
}


private [marlin]  object RandomRDD {


  def getPartitions[T](size: Long,
      numPartitions: Int,
      rng: RandomDataGenerator[T],
      seed: Long): Array[Partition] = {

    val partitions = new Array[RandomRDDPartition[T]](numPartitions)
    var i = 0
    var start: Long = 0
    var end: Long = 0
    val random = new Random(seed)
    while (i < numPartitions) {
      end = ((i + 1) * size) / numPartitions
      partitions(i) = new RandomRDDPartition(i, start, (end - start).toInt, rng, random.nextLong())
      start = end
      i += 1
    }
    partitions.asInstanceOf[Array[Partition]]
  }

  def getSparseVecIterator(
      partition: RandomRDDPartition[Double],
      vectorSize: Int,
      rowsLength: Long,
      density: Double): Iterator[(Long, BSV[Double])] = {
    val generator = partition.generator.copy()
    val rnd = new Random()
    generator.setSeed(partition.seed)
    val index = (0 + partition.start until rowsLength.toInt).toIterator
    val sparseSize = (vectorSize * density).toInt
    val set = new HashSet[Int]()
    while (set.size < sparseSize) {
      set.+=(rnd.nextInt(vectorSize))
    }
    val indexes = set.toArray
    scala.util.Sorting.quickSort(indexes)
    Iterator.fill(partition.size)((index.next(),
      new BSV[Double](indexes, Array.fill(sparseSize)(generator.nextValue()), vectorSize)))
  }


  // The RNG has to be reset every time the iterator is requested to guarantee same data
  // every time the content of the RDD is examined.
  def getDenseVecIterator(
      partition: RandomRDDPartition[Double],
      vectorSize: Int,
      rowsLength: Long): Iterator[(Long, BDV[Double])] = {
    val generator = partition.generator.copy()
    generator.setSeed(partition.seed)
    val index = (0 + partition.start until rowsLength.toInt).toIterator
    Iterator.fill(partition.size)((index.next(),
      BDV(Array.fill(vectorSize)(generator.nextValue()))))
  }

  // The RNG has to be reset every time the iterator is requested to guarantee same data
  // every time the content of the RDD is examined.
  def getBlockIterator(
      partition: RandomRDDPartition[Double],
      rows: Int,
      columns: Int,
      array: Array[BlockID],
      sparseInfo: (Boolean, Double)): Iterator[(BlockID, SubMatrix)] = {
    val generator = partition.generator.copy()
    generator.setSeed(partition.seed)
//    val arrayIt = array.slice(0 + partition.index * partition.size, array.length).toIterator
    val arrayIt = array.slice(0 + partition.start.toInt, array.length).toIterator
    val (isSparse, sparsity) = sparseInfo
    if(!isSparse) {
      Iterator.fill(partition.size)(arrayIt.next(),
        new SubMatrix(denseMatrix = BDM.create[Double](rows, columns, Array.fill(rows * columns)(generator.nextValue()))))
    }else {
      Iterator.fill(partition.size)(arrayIt.next(),
       new SubMatrix(spMatrix = SparseMatrix.rand(rows, columns, sparsity)))
    }
  }

  def getDistVectorIterator(
      partition: RandomRDDPartition[Double],
      vectorLength: Int,
      numSplit: Int): Iterator[(Int, DenseVector)] = {
    val generator = partition.generator.copy()
    generator.setSeed(partition.seed)
    val index = (0 + partition.start until numSplit).toIterator
    Iterator.fill(partition.size)(index.next().toInt,
      Vectors.dense(Array.fill(vectorLength)(generator.nextValue())))
  }
}


private[marlin] class RandomDistVectorRDD(@transient sc: SparkContext,
    length: Long,
    numSplits: Int,
    @transient rng: RandomDataGenerator[Double],
    @transient seed: Long = System.nanoTime()) extends RDD[(Int, DenseVector)](sc, Nil){
  @DeveloperApi
  override def compute(splitIn: Partition, context: TaskContext): Iterator[(Int, DenseVector)] = {
    val split = splitIn.asInstanceOf[RandomRDDPartition[Double]]
    var splitLength = math.ceil(length.toDouble / numSplits.toDouble).toInt
    if (splitIn.index == numSplits - 1){
      splitLength = length.toInt - splitLength * splitIn.index
    }
    RandomRDD.getDistVectorIterator(split, splitLength, numSplits)
  }

  override protected def getPartitions: Array[Partition] = {
    RandomRDD.getPartitions(numSplits, numSplits, rng, seed)
  }
}

private[marlin] class RandomSpaVecRDD(@transient sc: SparkContext,
    nRows: Long,
    vectorSize: Int,
    numPartitions: Int,
    @transient rng: RandomDataGenerator[Double],
    density: Double,
    @transient seed: Long = System.nanoTime()) extends RDD[(Long, BSV[Double])](sc, Nil){

  require(nRows > 0, "Positive RDD size required.")
  require(numPartitions > 0, "Positive number of partitions required")
  require(vectorSize > 0, "Positive vector size required.")
  require(density > 0, "Positive density")
  require(math.ceil(nRows.toDouble / numPartitions) <= Int.MaxValue,
    "Partition size cannot exceed Int.MaxValue")

  override def compute(splitIn: Partition, context: TaskContext): Iterator[(Long, BSV[Double])] = {
    val split = splitIn.asInstanceOf[RandomRDDPartition[Double]]
    RandomRDD.getSparseVecIterator(split, vectorSize, nRows, density)
  }

  override protected def getPartitions: Array[Partition] = {
    RandomRDD.getPartitions(nRows, numPartitions, rng, seed)
  }
}

private[marlin] class RandomDenVecRDD(@transient sc: SparkContext,
    nRows: Long,
    vectorSize: Int,
    numPartitions: Int,
    @transient rng: RandomDataGenerator[Double],
    @transient seed: Long = System.nanoTime()) extends RDD[(Long, BDV[Double])](sc, Nil){

  require(nRows > 0, "Positive RDD size required.")
  require(numPartitions > 0, "Positive number of partitions required")
  require(vectorSize > 0, "Positive vector size required.")
  require(math.ceil(nRows.toDouble / numPartitions) <= Int.MaxValue,
    "Partition size cannot exceed Int.MaxValue")

  override def compute(splitIn: Partition, context: TaskContext): Iterator[(Long, BDV[Double])] = {
    val split = splitIn.asInstanceOf[RandomRDDPartition[Double]]
    RandomRDD.getDenseVecIterator(split, vectorSize, nRows)
  }

  override protected def getPartitions: Array[Partition] = {
    RandomRDD.getPartitions(nRows, numPartitions, rng, seed)
  }
}

private [marlin] class RandomBlockRDD(@transient sc: SparkContext,
    nRows: Long,
    nColumns: Long,
    blksByRow: Int,
    blksByCol: Int,
    @transient rng: RandomDataGenerator[Double],
    @transient seed: Long = System.nanoTime(),
    sparseInfo: (Boolean, Double)) extends RDD[(BlockID, SubMatrix)](sc, Nil){

  require(blksByRow > 0 && blksByCol > 0, "Positive number of partitions required.")
  require(nRows > 0 && nColumns > 0, "Positive number of partitions required.")

  override def compute(splitIn: Partition, context: TaskContext): Iterator[(BlockID, SubMatrix)] = {
    val split = splitIn.asInstanceOf[RandomRDDPartition[Double]]
    val array = Array.ofDim[BlockID](blksByRow * blksByCol)
    for (i <- 0 until blksByRow){
      for (j <- 0 until blksByCol){
        array( i * blksByCol + j) = new BlockID(i, j)
      }
    }
    var blockRows = math.ceil(nRows.toDouble / blksByRow.toDouble).toInt
    if (splitIn.index >=  (blksByRow - 1)* blksByCol){
      if (blockRows * blksByRow > nRows) {
        blockRows = nRows.toInt - blockRows * (blksByRow - 1)
      }
    }
    var blockCols = math.ceil(nColumns.toDouble / blksByCol.toDouble).toInt
    if ((splitIn.index+1) % blksByCol == 0) {
      if (blockCols * blksByCol > nColumns) {
        blockCols = nColumns.toInt - blockCols * (blksByCol - 1)
      }
    }

    RandomRDD.getBlockIterator(split ,blockRows ,blockCols , array, sparseInfo)
  }

  override protected def getPartitions: Array[Partition] = {
    RandomRDD.getPartitions(blksByRow * blksByCol, blksByRow * blksByCol, rng, seed)
  }
}
