package edu.nju.pasalab.sparkmatrix

import breeze.linalg.{DenseMatrix => BDM}

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * BlockMatrix representing several [[breeze.linalg.DenseMatrix]] make up the matrix
 * with BlockID, every block should have the same rows and columns
 *
 * @param blocks blocks of this matrix
 * @param nRows number of rows
 * @param nCols number of columns
 * @param blksByRow block nums by row
 */

class BlockMatrix(
    val blocks: RDD[Block],
    private var nRows: Long,
    private var nCols: Long,
    private var blksByRow: Int,
    private var blksByCol: Int) extends DistributedMatrix{



  /** Alternative constructor leaving matrix dimensions to be determined automatically. */
  def this(blocks: RDD[Block]) = this(blocks, 0L, 0L, 0, 0)


  /** Gets or computes the number of rows. */
  override def numRows(): Long = {
    if (nRows <= 0L){
      nRows = this.blocks.filter(_.blockID.column == 0).map(_.matrix.rows).reduce(_ + _)
    }
    nRows
  }

  /** Gets or computes the number of columns. */
  override def numCols(): Long = {
    if (nCols <= 0L){
      nCols = this.blocks.filter(_.blockID.row == 0).map(_.matrix.cols).reduce(_ + _)
    }
    nCols
  }

  /** Gets or computes the number of blocks by the direction of row. */
  def numBlksByRow(): Int = {
    if (blksByRow <= 0L){
      blksByRow = this.blocks.filter(_.blockID.column == 0).count().toInt
    }
    blksByRow
  }

  /** Gets or computes the number of blocks by the direction of column. */
  def numBlksByCol(): Int = {
    if (blksByCol <= 0L){
      blksByCol = this.blocks.filter(_.blockID.row == 0).count().toInt
    }
    blksByCol
  }

  /** Collects data and assembles a local dense breeze matrix (for test only). */
  override private[sparkmatrix] def toBreeze(): BDM[Double] = ???

  /**
   * Save the result to the HDFS
   *
   * @param path the path to store the IndexMatrix in HDFS
   */
  def saveToFileSystem(path: String){
    this.toIndexmatrix().rows.saveAsTextFile(path)
  }


  /**
   * transform the BlockMatrix to IndexMatrix
   *
   * @return IndexMatrix with the same content
   */
  def toIndexmatrix(): IndexMatrix = {
    val mostBlockRowLen = math.ceil( numRows().toDouble / numBlksByRow().toDouble).toInt
    val mostBlockColLen = math.ceil( numCols().toDouble / numBlksByCol().toDouble).toInt

    val result = this.blocks.flatMap( t => {
      val smRows = t.matrix.rows
      val smCols = t.matrix.cols
      val array = t.matrix.data
      val arrayBuf = Array.ofDim[(Int, (Int, Array[Double]))](smRows)
      for ( i <- 0 until smRows){
        val tmp = Array.ofDim[Double](smCols)
        for (j <- 0 until tmp.length){
          tmp(j) = array(j * smRows + i)
        }
        arrayBuf(i) = ( t.blockID.row * mostBlockRowLen + i, (t.blockID.column, tmp) )
      }
      arrayBuf
    }).groupByKey()
      .map(input => {
      val array = Array.ofDim[Double](this.numCols().toInt)
      for (it <- input._2) {
        val colStart = mostBlockColLen * it._1
        for ( i <- 0 until it._2.length){
          array( colStart + i ) = it._2(i)
        }
      }
      new IndexRow(input._1 , Vectors.dense(array))
    })

    new IndexMatrix(result)
  }
}
