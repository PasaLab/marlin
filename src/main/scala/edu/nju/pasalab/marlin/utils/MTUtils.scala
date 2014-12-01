package edu.nju.pasalab.marlin.utils

import java.nio.ByteBuffer

import scala.util.hashing.MurmurHash3

import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

import edu.nju.pasalab.marlin.matrix._
import edu.nju.pasalab.marlin.rdd.RandomRDDs

object MTUtils {

  /** Hash seeds to have 0/1 bits throughout. */
  private[marlin] def hashSeed(seed: Long): Long = {
    val bytes = ByteBuffer.allocate(java.lang.Long.SIZE).putLong(seed).array()
    MurmurHash3.bytesHash(bytes)
  }

  /**
   * Function to generate a distributed BlockMatrix, in which every element is in the uniform distribution
   *
   * @param sc spark context
   * @param nRows the number of rows of the whole matrix
   * @param nColumns the number of columns of the whole matrix
   * @param numByRow the number of submatrices along the row side you assign, the real num may be not.
   * @param numByCol the number of submatrices along the column side you assign, the real num may be not.
   * @param distribution the distribution of the elements in the matrix, default is U[0.0, 1.0]
   * @return BlockMatrix
   */
  def randomBlockMatrix(sc: SparkContext,
      nRows: Long,
      nColumns: Int,
      numByRow: Int,
      numByCol: Int,
      distribution: RandomDataGenerator[Double] = new UniformGenerator(0.0, 1.0)): BlockMatrix = {

    val mRows = nRows.toInt
    val mColumns = nColumns
    val mBlockRowSize = math.ceil(mRows.toDouble / numByRow.toDouble).toInt
    val mBlockColSize = math.ceil(mColumns.toDouble / numByCol.toDouble).toInt
    val blksByRow = math.ceil(mRows.toDouble / mBlockRowSize).toInt
    val blksByCol = math.ceil(mColumns.toDouble / mBlockColSize).toInt

    val blocks = RandomRDDs.randomBlockRDD(sc, distribution, nRows, nColumns, blksByRow, blksByCol)
    new BlockMatrix(blocks, nRows, nColumns, blksByRow, blksByCol)
  }

  /**
   * Function to generate a distributed BlockMatrix, in which every element is in the uniform distribution
   *
   * @param sc spark context
   * @param nRows the number of rows of the whole matrix
   * @param nColumns the number of columns of the whole matrix
   * @param distribution the distribution of the elements in the matrix, default is U[0.0, 1.0]
   * @return DenseVecMatrix
   */
  def randomDenVecMatrix(sc: SparkContext,
      nRows: Long,
      nColumns: Int,
      distribution: RandomDataGenerator[Double] = new UniformGenerator(0.0, 1.0),
      numPartitions: Int = 0): DenseVecMatrix = {
    
    val rows = RandomRDDs.randomDenVecRDD(sc, distribution, nRows, nColumns, numPartitionsOrDefault(sc, numPartitions))
    new DenseVecMatrix(rows, nRows, nColumns)  
  }

  /**
   *  Function to generate a distributed BlockMatrix, in which every element is zero
   *
   * @param sc spark context
   * @param nRows the number of rows of the whole matrix
   * @param nColumns the number of columns of the whole matrix
   * @return DenseVecMatrix
   */
  def zerosDenVecMatrix(sc: SparkContext,
      nRows: Long,
      nColumns: Int): DenseVecMatrix = {

    val rows = RandomRDDs.zerosDenVecRDD(sc, nRows, nColumns)
    new DenseVecMatrix(rows, nRows, nColumns)
  }

  /**
   *  Function to generate a distributed BlockMatrix, in which every element is one
   *
   * @param sc spark context
   * @param nRows the number of rows of the whole matrix
   * @param nColumns the number of columns of the whole matrix
   * @return DenseVecMatrix
   */
  def onesDenVecMatrix(sc: SparkContext,
      nRows: Long,
      nColumns: Int): DenseVecMatrix = {

    val rows = RandomRDDs.onesDenVecRDD(sc, nRows, nColumns)
    new DenseVecMatrix(rows, nRows, nColumns)
  }

  /**
   * Function to design the method how to split input two matrices
   *
   * @param m rows num of Matrix A
   * @param k columns num of Matrix A, also rows num of Matrix B
   * @param n columns num of Matrix B
   * @param cores all the cores cross the cluster
   * @return rows of Matrix A to be split nums, columns of Matrix A to be split nums,
   *         columns of Matrix B to be split nums
   */
  private[marlin] def splitMethod(m: Long, k: Long, n: Long, cores: Int): (Int, Int, Int) = {
    var mSplitNum = 1
    var kSplitNum = 1
    var nSplitNum = 1
    var _m = m
    var _k = k
    var _n = n
    var _cores = cores
    val factor = 2
    while (_cores > 1 && _m > 1 && _k > 1 && _n > 1) {
      if (dimToSplit(_m, _k, _n) == 1) {
        nSplitNum *= factor
        _n /= factor
        _cores /= factor
      } else if (dimToSplit(_m, _k, _n) == 2) {
        mSplitNum *= factor
        _m /= factor
        _cores /= factor
      } else {
        kSplitNum *= factor
        _k /= factor
        _cores /= factor
      }
    }
    (mSplitNum, kSplitNum, nSplitNum)
  }

  private def dimToSplit(m: Long, k: Long, n: Long): Int={
    var result = 0
    if (n >= k && n >= m) {
      result = 1
    }else if (m >= k && m >= n) {
      result = 2
    }
    else result = 3
    result
  }

  /**
   * Function to load matrix from file
   *
   * @param sc the running SparkContext
   * @param path the path where store the matrix
   * @param minPartitions the min num of partitions of the matrix to load in Spark
   * @return a distributed matrix in DenseVecMatrix type                    
   */
  def loadMatrixFile(sc: SparkContext, path: String, minPartitions: Int = 4): DenseVecMatrix = {
    if (!path.startsWith("hdfs://") && !path.startsWith("tachyon://") && !path.startsWith("file://")) {
      System.err.println("the path is not in local file System, HDFS or Tachyon")
      System.exit(1)
    }
    val file = sc.textFile(path, minPartitions)
    val rows = file.map(t =>{
      val e = t.split(":")
      val rowIndex = e(0).toLong
      val array = e(1).split(",\\s?|\\s+").map(_.toDouble)
      val vec = Vectors.dense(array)
      (rowIndex,vec)
    })
    new DenseVecMatrix(rows)
  }

  /**
   * Load DenseVecMatrix from sequence file, the original sequenceFile is key-value pair stored,
   * key is index in `Long` type, value is `DenseVector`
   * s
   *@param sc the running SparkContext
   * @param path the path where store the matrix
   * @param minPartitions the min num of partitions of the matrix to load in Spark
   */
  def loadMatrixSeqFile(sc: SparkContext, path: String, minPartitions: Int = 0): DenseVecMatrix = {
    val result = sc.sequenceFile[Long, DenseVector](path, numPartitionsOrDefault(sc, minPartitions))
    new DenseVecMatrix(result)
  }


  /**
   * Function to load block matrix from file
   *
   * @param sc the running SparkContext
   * @param path the path where store the matrix
   * @param minPartitions the min num of partitions of the matrix to load in Spark
   * @return a distributed matrix in BlockMatrix type 
   */
  def loadBlockMatrixFile(sc: SparkContext, path: String, minPartitions: Int = 4): BlockMatrix = {
    if (!path.startsWith("hdfs://") && !path.startsWith("tachyon://") && !path.startsWith("file://")) {
      System.err.println("the path is not in local file System, HDFS or Tachyon")
      System.exit(1)
    }
    val file = sc.textFile(path, minPartitions)
    println("RDD length: " + file.count())
    val blocks = file.map(t =>{
        val e = t.split(":")
        val info = e(0).split("-")
        val array = e(1).split(",\\s?|\\s+").map(_.toDouble)
        (new BlockID(info(0).toInt, info(1).toInt), new BDM[Double](info(2).toInt, info(3).toInt, array))
      })
    new BlockMatrix(blocks)
  }

  /**
   * Function to load matrix from a dictionary which contains many files to generate a Matrix
   *
   * @param sc the running SparkContext
   * @param path the path where store the matrix
   * @param minPartitions the min num of partitions of the matrix to load in Spark
   * @return a distributed matrix in DenseVecMatrix type                    
   */
  def loadMatrixFiles(sc: SparkContext, path: String, minPartitions: Int = 4): DenseVecMatrix = {
    if (!path.startsWith("hdfs://") && !path.startsWith("tachyon://") && !path.startsWith("file://")) {
      System.err.println("the path is not in local file System, HDFS or Tachyon")
      System.exit(1)
    }
    val files = sc.wholeTextFiles(path, minPartitions)
    val rows = files.filter(t => !t._2.isEmpty).flatMap(t =>{
      val lines = t._2.split("\n")
      lines.map( l =>{
        val content = l.split(":")
        ( content(0).toLong, Vectors.dense( content(1).split(",\\s?|\\s+").map(_.toDouble) ))
      })
    })
    new DenseVecMatrix(rows)
  }

  /**
   * Function to load block matrix from a dictionary which contains many files to generate a Matrix
   *
   * @param sc the running SparkContext
   * @param path the path where store the matrix
   * @param minPartitions the min num of partitions of the matrix to load in Spark
   * @return a disrtibuted matrix in BlockMatrix type                    
   */
  def loadBlockMatrixFiles(sc: SparkContext, path: String, minPartitions: Int = 4): BlockMatrix = {
    if (!path.startsWith("hdfs://") && !path.startsWith("tachyon://") && !path.startsWith("file://")) {
      System.err.println("the path is not in local file System, HDFS or Tachyon")
      throw new IllegalArgumentException()
    }
    val file = sc.wholeTextFiles(path, minPartitions)
    val blocks = file.flatMap(t =>{
      val blks = t._2.split("\n")
      blks.map( b =>{
        val e = b.split(":")
        val info = e(0).split("-")
        val array = e(1).split(",\\s?|\\s+").map(_.toDouble)
        (new BlockID(info(0).toInt, info(1).toInt), new BDM[Double](info(2).toInt, info(3).toInt, array))
      })
    })
    new BlockMatrix(blocks)
  }

  /**
   * Function to generate a DenseVecMatrix from a Array[Array[Double]
   *
   * @param sc the running SparkContext
   * @param array the two dimension Double array
   * @param partitions the default num of partitions when you create an RDD, you can set it by yourself
   * @return a distributed matrix in DenseVecMatrix type                  
   */
  def arrayToMatrix(sc:SparkContext , array: Array[Array[Double]] , partitions: Int = 2): DenseVecMatrix ={
    new DenseVecMatrix( sc.parallelize(array.zipWithIndex
      .map{case(t,i)  => (i.toLong, Vectors.dense(t)) },partitions) )
  }

  /**
   * Function to transform a DenseVecMatrix to a Array[Array[Double]], but now we use collect method to
   * get all the content from distributed matrices which means it is expensive, so currently only
   * not so large matrix can transform to Array[Array[Double]]
   *
   * @param mat the DenseVecMatrix to be transformed
   * @return a local array of two dimensions           
   */

  def matrixToArray(mat: DistributedMatrix ): Array[Array[Double]] ={
    mat match {
      case m: DenseVecMatrix => {
        val array = Array.ofDim[Double](m.numRows().toInt, m.numCols().toInt)
        m.rows.collect().foreach(t => t._2.toArray.copyToArray(array(t._1.toInt)) )
        array
      }
      case m: BlockMatrix => {
        val blkRowSize = math.ceil(m.numRows().toDouble / m.numBlksByRow().toDouble).toInt
        val blkColSize = math.ceil(m.numCols().toDouble / m.numBlksByCol().toDouble).toInt
        val array = Array.ofDim[Double](m.numRows().toInt, m.numCols().toInt)
        m.blocks.collect().foreach(t => {
          val rowBase = t._1.row * blkRowSize
          val colBase = t._1.column * blkColSize
          val iterator = t._2.iterator
          while (iterator.hasNext) {
            val ((r, c), v) = iterator.next()
            array(r + rowBase)(c + colBase) = v
          }
        })
        array
      }
    }

  }

  /**
   * This function is used to improve the matrix multiply performance, just broadcast a local
   * matrix, avoid the overhead when collecting the distributed matrix
   *
   * @param localMat a local breeze.linalg.DenseMatrix
   * @param matrix the distributed matrix
   * @param cores the num of cores across the cluster
   * @return
   */
  def leftBroadMultiply(localMat: BDM[Double], matrix: DistributedMatrix, cores: Int): BlockMatrix = {
    require(localMat.cols == matrix.numRows(), s"matrices dimension mismatched: ${localMat.cols} vs ${matrix.numRows()}")
    matrix match {
      case that: BlockMatrix => {
        val brodMat = that.blocks.context.broadcast(localMat)
        val result = that.blocks.mapPartitions(iter => {
          iter.map( t => {
            (t._1, (brodMat.value * t._2).asInstanceOf[BDM[Double]])
          })
        })
        new BlockMatrix(result, localMat.rows, that.numCols(), that.numBlksByRow(), that.numBlksByCol())
      }
      case that: DenseVecMatrix => {
        val brodMat = that.rows.context.broadcast(localMat)
        val blkMat = that.toBlockMatrix(1, math.min(8 * cores, that.numCols().toInt / 2))
        val result = blkMat.blocks.mapPartitions(iter => {
          iter.map( t => {
            (t._1, (brodMat.value * t._2).asInstanceOf[BDM[Double]])
          })
        })
        new BlockMatrix(result, localMat.rows, that.numCols(), 1, math.min(8 * cores, that.numCols().toInt / 2))
      }
    }
  }


  /**
   * Returns `numPartitions` if it is positive, or `sc.defaultParallelism` otherwise.
   */
  private def numPartitionsOrDefault(sc: SparkContext, numPartitions: Int): Int = {
    if (numPartitions > 0) numPartitions
    else if (!sc.getConf.getOption("spark.default.parallelism").isEmpty) {
      sc.getConf.get("spark.default.parallelism").toInt
    }else 
      sc.defaultMinPartitions
  }


}