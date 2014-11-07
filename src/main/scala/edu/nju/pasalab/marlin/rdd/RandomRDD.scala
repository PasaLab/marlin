package edu.nju.pasalab.marlin.rdd

import scala.util.Random

import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}

import edu.nju.pasalab.marlin.matrix.{BlockID, DenseVector}
import edu.nju.pasalab.marlin.utils.RandomDataGenerator

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
  
  // The RNG has to be reset every time the iterator is requested to guarantee same data
  // every time the content of the RDD is examined.
  def getDenseVecIterator(
      partition: RandomRDDPartition[Double],
      vectorSize: Int,
      rowsLength: Long): Iterator[(Long, DenseVector)] = {
    val generator = partition.generator.copy()
    generator.setSeed(partition.seed)
    val index = (0 + partition.start until rowsLength.toInt).toIterator
    Iterator.fill(partition.size)((index.next(),
      new DenseVector(Array.fill(vectorSize)(generator.nextValue()))))
  }

  // The RNG has to be reset every time the iterator is requested to guarantee same data
  // every time the content of the RDD is examined.
  def getBlockIterator(
      partition: RandomRDDPartition[Double],
      rows: Int,
      columns: Int,
      array: Array[BlockID]): Iterator[(BlockID, BDM[Double])] = {
    val generator = partition.generator.copy()
    generator.setSeed(partition.seed)
//    val arrayIt = array.slice(0 + partition.index * partition.size, array.length).toIterator
    val arrayIt = array.slice(0 + partition.start.toInt, array.length).toIterator
    Iterator.fill(partition.size)(arrayIt.next(),
      new BDM(rows, columns, Array.fill(rows*columns)(generator.nextValue())))
  }
}

private[marlin] class RandomDenVecRDD(@transient sc: SparkContext,
    nRows: Long,
    vectorSize: Int,
    numPartitions: Int,
    @transient rng: RandomDataGenerator[Double],
    @transient seed: Long = System.nanoTime()) extends RDD[(Long, DenseVector)](sc, Nil){

  require(nRows > 0, "Positive RDD size required.")
  require(numPartitions > 0, "Positive number of partitions required")
  require(vectorSize > 0, "Positive vector size required.")
  require(math.ceil(nRows.toDouble / numPartitions) <= Int.MaxValue,
    "Partition size cannot exceed Int.MaxValue")

  override def compute(splitIn: Partition, context: TaskContext): Iterator[(Long, DenseVector)] = {
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
    @transient seed: Long = System.nanoTime()) extends RDD[(BlockID, BDM[Double])](sc, Nil){

  require(blksByRow > 0 && blksByCol > 0, "Positive number of partitions required.")
  require(nRows > 0 && nColumns > 0, "Positive number of partitions required.")

  override def compute(splitIn: Partition, context: TaskContext): Iterator[(BlockID, BDM[Double])] = {
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

    RandomRDD.getBlockIterator(split ,blockRows ,blockCols , array)
  }

  override protected def getPartitions: Array[Partition] = {
    RandomRDD.getPartitions(blksByRow * blksByCol, blksByRow * blksByCol, rng, seed)
  }
}
