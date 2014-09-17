package edu.nju.pasalab.sparkmatrix

import breeze.linalg.{DenseMatrix => BDM}
import org.apache.log4j.{Level, Logger}

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * BlockMatrix representing several [[breeze.linalg.DenseMatrix]] make up the matrix
 * with BlockID
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
  override private[sparkmatrix] def toBreeze(): BDM[Double] = {
    val m = numRows().toInt
    val n = numCols().toInt
    val mostBlkRowLen =math.ceil(m.toDouble / blksByRow.toDouble).toInt
    val mostBlkColLen =math.ceil(n.toDouble / blksByCol.toDouble).toInt
    val mat = BDM.zeros[Double](m, n)
    blocks.collect().foreach{
      case Block(blkID, matrix) =>
        val rowStart = blkID.row
        val colStart = blkID.column
        matrix.activeIterator.foreach{
          case ((i, j), v) =>
            mat(rowStart * mostBlkRowLen + i, colStart * mostBlkColLen + j) = v
        }
    }
    mat
  }

  /**
   * matrix-matrix multiplication between two BlockMatrix
   * @param other the matrix to be multiplied
   * @return the multiplication result in BlockMatrix form
   */
  final def multiply(other: BlockMatrix): BlockMatrix = {

    require(this.numCols() == other.numRows(), s"Dimension mismatch: ${this.numCols()} vs ${other.numRows()}")
    //num of rows to be split of this matrix
    val mSplitNum = this.numBlksByRow()
    //num of columns to be split of this matrix, meanwhile num of rows of that matrix
    val kSplitNum = this.numBlksByCol()
    //num of columns to be split of that matrix
    val nSplitNum = other.numBlksByCol()
    val thisEmitBlocks = blocks.flatMap( t => {
      val array = Array.ofDim[(BlockID, BDM[Double])](nSplitNum)
      for (i <- 0 until nSplitNum){
        val seq = t.blockID.row * nSplitNum * kSplitNum + i * kSplitNum + t.blockID.column
        array(i) = (new BlockID(t.blockID.row, i, seq), t.matrix)
      }
      array
    })

    val otherEmitBlocks = other.blocks.flatMap( t =>{
      val array = Array.ofDim[(BlockID, BDM[Double])](mSplitNum)
      for (i <- 0 until mSplitNum){
        val seq = i * nSplitNum * kSplitNum + t.blockID.column * kSplitNum + t.blockID.row
        array(i) = (new BlockID(i, t.blockID.column, seq), t.matrix)
      }
      array
    })

    if ( kSplitNum == 1) {
      val result = thisEmitBlocks.join(otherEmitBlocks).map(t => {
        val b1 = t._2._1.asInstanceOf[BDM[Double]]
        val b2 = t._2._2.asInstanceOf[BDM[Double]]
        val t2 = System.currentTimeMillis()

        Logger.getLogger(this.getClass).log(Level.INFO, "b1 rows: " + b1.rows + " , b1 cols: " + b1.cols)
        Logger.getLogger(this.getClass).log(Level.INFO, "b2 rows: " + b2.rows + " , b2 cols: " + b2.cols)

        val c = (b1 * b2).asInstanceOf[BDM[Double]]

        val t3 = System.currentTimeMillis()

        Logger.getLogger(this.getClass).log(Level.INFO, "breeze multiply time: " + (t3 - t2).toString + " ms")
        new Block(t._1, c)
      }).cache()
      new BlockMatrix(result, this.numRows(), other.numCols(), mSplitNum, nSplitNum)
    }else{
      val result = thisEmitBlocks.join(otherEmitBlocks).map(t => {
        val b1 = t._2._1.asInstanceOf[BDM[Double]]
        val b2 = t._2._2.asInstanceOf[BDM[Double]]
        val t2 = System.currentTimeMillis()

        Logger.getLogger(this.getClass).log(Level.INFO, "b1 rows: " + b1.rows + " , b1 cols: " + b1.cols)
        Logger.getLogger(this.getClass).log(Level.INFO, "b2 rows: " + b2.rows + " , b2 cols: " + b2.cols)

        val c = (b1 * b2).asInstanceOf[BDM[Double]]

        val t3 = System.currentTimeMillis()

        Logger.getLogger(this.getClass).log(Level.INFO, "breeze multiply time: " + (t3 - t2).toString + " ms")
        (new BlockID(t._1.row, t._1.column), c)
      }).cache()
        .groupByKey()
        .map( t => {
        val example = t._2.head
        val smRows = example.rows
        val smCols = example.cols
        val t1 = System.currentTimeMillis()
        var mat = new BDM[Double](smRows, smCols)
        for ( m <- t._2){
          mat = mat + m.asInstanceOf[BDM[Double]]
        }
        Logger.getLogger(this.getClass).log(Level.INFO, "breeze add time: "
          + (System.currentTimeMillis() - t1).toString + " ms")
        new Block(t._1, mat)
      })
      new BlockMatrix(result, this.numRows(), other.numCols(), mSplitNum, nSplitNum)
    }
  }



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
//    blocks.cache()
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
