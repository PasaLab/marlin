package edu.nju.pasalab.marlin.rdd

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import edu.nju.pasalab.marlin.matrix.{SubMatrix, DenseVector, BlockID}
import edu.nju.pasalab.marlin.utils.{OnesGenerator, ZerosGenerator, RandomDataGenerator}

object RandomRDDs {
  def randomSpaVecRDD(sc: SparkContext,
                      generator: RandomDataGenerator[Double],
                      numRows: Long,
                      numCols: Int,
                      numPartitions: Int,
                      density: Double,
                      seed: Long = System.nanoTime()) = {
    new RandomSpaVecRDD(
      sc, numRows, numCols, numPartitions, generator, density, seed)
  }


  /**
   * Generates an RDD[(Long, DenseVector)] with vectors containing i.i.d. samples produced by the
   * input RandomDataGenerator.
   *
   * @param sc SparkContext used to create the RDD.
   * @param generator RandomDataGenerator used to populate the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @param seed Random seed (default: a random long integer).
   * @return RDD[(Long, DenseVector)] with vectors containing i.i.d. samples produced by generator.
   */
  def randomDenVecRDD(sc: SparkContext,
      generator: RandomDataGenerator[Double],
      numRows: Long,
      numCols: Int,
      numPartitions: Int,
      seed: Long = System.nanoTime()): RDD[(Long, BDV[Double])] = {
    new RandomDenVecRDD(
      sc, numRows, numCols, numPartitions, generator, seed)
  }

  def randomDistVectorRDD(sc: SparkContext,
      generator: RandomDataGenerator[Double],
      length: Long,
      numSplits: Int,
      seed: Long = System.nanoTime()): RDD[(Int, DenseVector)] = {
    new RandomDistVectorRDD(sc, length, numSplits, generator, seed)
  }

  /**
   * Generates an RDD[(Long, DenseVector)] with every elements in the vector is zero.
   *
   * @param sc SparkContext used to create the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @return
   */
  def zerosDenVecRDD(sc: SparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0): RDD[(Long, BDV[Double])] = {

    new RandomDenVecRDD(
      sc, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), new ZerosGenerator())
  }

  /**
   * Generates an RDD[(Long, DenseVector)] with every elements in the vector is one.
   *
   * @param sc SparkContext used to create the RDD.
   * @param numRows Number of Vectors in the RDD.
   * @param numCols Number of elements in each Vector.
   * @param numPartitions Number of partitions in the RDD (default: `sc.defaultParallelism`).
   * @return
   */
  def onesDenVecRDD(sc: SparkContext,
      numRows: Long,
      numCols: Int,
      numPartitions: Int = 0): RDD[(Long, BDV[Double])] = {

    new RandomDenVecRDD(
      sc, numRows, numCols, numPartitionsOrDefault(sc, numPartitions), new OnesGenerator())
  }



  /**
   * Generates an RDD[(BlockID, BDM[Double])] with elements containing i.i.d. samples produced by the
   * input RandomDataGenerator.
   *
   * @param sc SparkContext used to create the RDD.
   * @param generator RandomDataGenerator used to populate the RDD.
   * @param numRows Number of rows in the whole blockMatrix RDD.
   * @param numCols Number of columns in the whole blockMatrix RDD.
   * @param blksByRow Number of blocks in the RDD along the row side
   * @param blksByCol Number of blocks in the RDD along the column side
   * @param seed Random seed (default: a random long integer).
   * @return RDD[(BlockID, SubMatrix)] with vectors containing i.i.d. samples produced by generator.
   */
  def randomBlockRDD(sc: SparkContext,
      generator: RandomDataGenerator[Double],
      numRows: Long,
      numCols: Long,
      blksByRow: Int,
      blksByCol: Int,
      sparseInfo: (Boolean, Double),
      seed: Long = System.nanoTime()): RDD[(BlockID, SubMatrix)] = {
    //note: numPartitions must be divided by the numRows
    new RandomBlockRDD(
      sc, numRows, numCols, blksByRow, blksByCol, generator, seed, sparseInfo)
  }


  /**
   * Returns `numPartitions` if it is positive, or `sc.defaultParallelism` otherwise.
   */
  private def numPartitionsOrDefault(sc: SparkContext, numPartitions: Int): Int = {
    if (numPartitions > 0) numPartitions else sc.defaultMinPartitions
  }
  
}


