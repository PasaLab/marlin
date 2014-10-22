package edu.nju.pasalab.sparkmatrix

import breeze.linalg.{DenseMatrix => BDM}

import org.apache.spark.HashPartitioner
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
    val blocks: RDD[(BlockID, BDM[Double])],
    private var nRows: Long,
    private var nCols: Long,
    private var blksByRow: Int,
    private var blksByCol: Int) extends DistributedMatrix{



  /** Alternative constructor leaving matrix dimensions to be determined automatically. */
  def this(blocks: RDD[(BlockID, BDM[Double])]) = this(blocks, 0L, 0L, 0, 0)


  /** Gets or computes the number of rows. */
  override def numRows(): Long = {
    if (nRows <= 0L){
      nRows = this.blocks.filter(_._1.column == 0).map(_._2.rows).reduce(_ + _)
    }
    nRows
  }

  /** Gets or computes the number of columns. */
  override def numCols(): Long = {
    if (nCols <= 0L){
      nCols = this.blocks.filter(_._1.row == 0).map(_._2.cols).reduce(_ + _)
    }
    nCols
  }

  /** Gets or computes the number of blocks by the direction of row. */
  def numBlksByRow(): Int = {
    if (blksByRow <= 0L){
      blksByRow = this.blocks.filter(_._1.column == 0).count().toInt
    }
    blksByRow
  }

  /** Gets or computes the number of blocks by the direction of column. */
  def numBlksByCol(): Int = {
    if (blksByCol <= 0L){
      blksByCol = this.blocks.filter(_._1.row == 0).count().toInt
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
      case (blkID, matrix) =>
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
   * @param cores all the num of cores across the cluster
   * @return the multiplication result in BlockMatrix type
   */
  final def multiply(other: BlockMatrix, cores: Int): BlockMatrix = {

    require(this.numCols() == other.numRows(), s"Dimension mismatch: ${this.numCols()} vs ${other.numRows()}")
    if ( this.numBlksByCol() != other.numBlksByRow()){
      this.toDenseVecMatrix().multiply(other.toDenseVecMatrix(), cores)
    }else {
      //num of rows to be split of this matrix
      val mSplitNum = this.numBlksByRow()
      //num of columns to be split of this matrix, meanwhile num of rows of that matrix
      val kSplitNum = this.numBlksByCol()
      //num of columns to be split of that matrix
      val nSplitNum = other.numBlksByCol()
      val partitioner = new HashPartitioner(2 * cores)
      if (mSplitNum == 1 && nSplitNum == 1) {
        val result = blocks.join(other.blocks)
          .mapPartitions({ iter =>
          iter.map { t =>
            val b1 = t._2._1.asInstanceOf[BDM[Double]]
            val b2 = t._2._2.asInstanceOf[BDM[Double]]
            val c = (b1 * b2).asInstanceOf[BDM[Double]]
            (new BlockID(t._1.row, t._1.column), c)
          }
        }).partitionBy(partitioner).persist().reduceByKey(_ + _)
        new BlockMatrix(result, this.numRows(), other.numCols(), mSplitNum, nSplitNum)
      } else {
        val thisEmitBlocks = blocks.mapPartitions({ iter =>
          iter.flatMap(t => {
            val array = Array.ofDim[(BlockID, BDM[Double])](nSplitNum)
            for (i <- 0 until nSplitNum) {
              val seq = t._1.row * nSplitNum * kSplitNum + i * kSplitNum + t._1.column
              array(i) = (new BlockID(t._1.row, i, seq), t._2)
            }
            array
          })
        }).partitionBy(partitioner).cache()
        val otherEmitBlocks = other.blocks.mapPartitions({ iter =>
          iter.flatMap(t => {
            val array = Array.ofDim[(BlockID, BDM[Double])](mSplitNum)
            for (i <- 0 until mSplitNum) {
              val seq = i * nSplitNum * kSplitNum + t._1.column * kSplitNum + t._1.row
              array(i) = (new BlockID(i, t._1.column, seq), t._2)
            }
            array
          })
        }).partitionBy(partitioner).cache()
        if (kSplitNum != 1) {
          val result = thisEmitBlocks.join(otherEmitBlocks)
            .mapPartitions({ iter =>
            iter.map { t =>
              val b1 = t._2._1.asInstanceOf[BDM[Double]]
              val b2 = t._2._2.asInstanceOf[BDM[Double]]
              val c = (b1 * b2).asInstanceOf[BDM[Double]]
              (new BlockID(t._1.row, t._1.column), c)
            }
          }).partitionBy(partitioner).persist().reduceByKey(_ + _)
          new BlockMatrix(result, this.numRows(), other.numCols(), mSplitNum, nSplitNum)
        } else {
          val result = thisEmitBlocks.join(otherEmitBlocks)
            .mapValues(t => (t._1.asInstanceOf[BDM[Double]] * t._2.asInstanceOf[BDM[Double]])
            .asInstanceOf[BDM[Double]])
          new BlockMatrix(result, this.numRows(), other.numCols(), mSplitNum, nSplitNum)
        }
      }
    }
  }

  /**
   * BlockMatrix add another
   * @param other the matrix to be added in DenseVecMatrix type
   * @return the addition result in DenseVecMatrix type
   */
  final def add(other: DenseVecMatrix): DenseVecMatrix = {
    this.toDenseVecMatrix().add(other)
  }

  /**
   *  A transposed view of BlockMatrix
   *  @return the transpose of this BlockMatrix
   */
  final def transpose(): BlockMatrix = {
    val result = blocks.mapPartitions( iter =>{
      iter.map ( t =>{
        (new BlockID(t._1.column, t._1.row), t._2.t )
      })
    })
    new BlockMatrix(result, this.numRows(), this.numCols(), this.numBlksByRow(), this.numBlksByCol())
  }

  /**
   * Using spark-property broadcast to decrease time used in the matrix-matrix multiplication
   * @param other other matrix to be multiplied
   * @return
   */
  def multiplyBroadcast(other: BlockMatrix, parallelism: Int, splits:(Int, Int, Int), mode: String): BlockMatrix = {
    val bArr = if (mode.toLowerCase.equals("broadcastb")) {
      val subBlocks = other.blocks.collect()
      val blocksArray =  Array.ofDim[BDM[Double]](other.blksByRow, other.blksByCol)
      for (s <- subBlocks) {
        blocksArray(s._1.row)(s._1.column) = s._2
      }
      blocks.context.broadcast(blocksArray)
    }else {
      val subBlocks = blocks.collect()
      val blocksArray = Array.ofDim[BDM[Double]](blksByRow, blksByCol)
      for (s <- subBlocks) {
        blocksArray(s._1.row)(s._1.column) = s._2
      }
      blocks.context.broadcast(blocksArray)
    }

      if (splits._2 == 1) {
        val result = blocks.mapPartitions(iter => {
          val res = Array.ofDim[(BlockID, BDM[Double])](other.blksByCol)
          val mats = bArr.value
          iter.flatMap(t => {
            for (j <- 0 until other.blksByCol) {
              if (mode.toLowerCase.equals("broadcastb")) {
                res(j) = (new BlockID(t._1.row, j),
                  (t._2 * mats(t._1.column)(j)).asInstanceOf[BDM[Double]])
              }else {
                res(j) = (new BlockID(t._1.row, j),
                  (mats(t._1.column)(j) * t._2).asInstanceOf[BDM[Double]])
              }
            }
            res
          })
        })
        new BlockMatrix(result, this.numRows(), other.numCols(),
          this.numBlksByRow(), other.numBlksByCol())
      } else {
        val result = blocks.mapPartitions(iter => {
          val res = Array.ofDim[(BlockID, BDM[Double])](other.blksByCol)
          val mats = bArr.value
          iter.flatMap(t => {
            for (j <- 0 until other.blksByCol) {
              if (mode.toLowerCase.equals("broadcastb")) {
                res(j) = (new BlockID(t._1.row, j),
                  (t._2 * mats(t._1.column)(j)).asInstanceOf[BDM[Double]])
              }else {
                res(j) = (new BlockID(t._1.row, j),
                  (mats(t._1.column)(j) * t._2).asInstanceOf[BDM[Double]])
              }
            }
            res
          })
        }).partitionBy(new HashPartitioner(parallelism)).cache().reduceByKey(_ + _)
        new BlockMatrix(result, this.numRows(), other.numCols(),
          this.numBlksByRow(), other.numBlksByCol())
      }
  }
  
  /**
   * Save the result to the HDFS
   *
   * @param path the path to store in HDFS
   * @param format if set "blockmatrix", it will store in the format of [[edu.nju.pasalab.sparkmatrix.BlockMatrix]]
   *               , else it will store in the format of [[edu.nju.pasalab.sparkmatrix.DenseVecMatrix]]
   */
  def saveToFileSystem(path: String, format: String = " "){
    if (format.toLowerCase.equals("blockmatrix")){
      this.blocks.saveAsTextFile(path)
    }else {
      this.toDenseVecMatrix().rows.saveAsTextFile(path)
    }
  }




  /**
   * transform the BlockMatrix to IndexMatrix
   *
   * @return IndexMatrix with the same content
   */
  def toDenseVecMatrix(): DenseVecMatrix = {
    val mostBlockRowLen = math.ceil( numRows().toDouble / numBlksByRow().toDouble).toInt
    val mostBlockColLen = math.ceil( numCols().toDouble / numBlksByCol().toDouble).toInt
    //    blocks.cache()
    val result = this.blocks.flatMap( t => {
      val smRows = t._2.rows
      val smCols = t._2.cols
      val array = t._2.data
      val arrayBuf = Array.ofDim[(Int, (Int, Array[Double]))](smRows)
      for ( i <- 0 until smRows){
        val tmp = Array.ofDim[Double](smCols)
        for (j <- 0 until tmp.length){
          tmp(j) = array(j * smRows + i)
        }
        arrayBuf(i) = ( t._1.row * mostBlockRowLen + i, (t._1.column, tmp) )
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

    new DenseVecMatrix(result)
  }




}


