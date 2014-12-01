package edu.nju.pasalab.marlin.matrix

import breeze.linalg.{DenseMatrix => BDM}
import org.apache.hadoop.io.{Text, NullWritable}
import org.apache.hadoop.mapred.TextOutputFormat
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
      nRows = blocks.filter(_._1.column == 0).map(_._2.rows).reduce(_ + _)
    }
    nRows
  }

  /** Gets or computes the number of columns. */
  override def numCols(): Long = {
    if (nCols <= 0L){
      nCols = blocks.filter(_._1.row == 0).map(_._2.cols).reduce(_ + _)
    }
    nCols
  }

  /** Gets or computes the number of blocks by the direction of row. */
  def numBlksByRow(): Int = {
    if (blksByRow <= 0L){
      blksByRow = blocks.filter(_._1.column == 0).count().toInt
    }
    blksByRow
  }

  /** Gets or computes the number of blocks by the direction of column. */
  def numBlksByCol(): Int = {
    if (blksByCol <= 0L){
      blksByCol = blocks.filter(_._1.row == 0).count().toInt
    }
    blksByCol
  }

  /** Collects data and assembles a local dense breeze matrix (for test only). */
  override private[matrix] def toBreeze(): BDM[Double] = {
    val m = numRows().toInt
    val n = numCols().toInt
    val mostBlkRowLen =math.ceil(m.toDouble / numBlksByRow().toDouble).toInt
    val mostBlkColLen =math.ceil(n.toDouble / numBlksByCol().toDouble).toInt
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
  final def multiply(other: DistributedMatrix, cores: Int): BlockMatrix = {

    require(numCols() == other.numRows(), s"Dimension mismatch: ${numCols()} vs ${other.numRows()}")
    other match {
      case mat: BlockMatrix => {

        if (numBlksByCol() != mat.numBlksByRow()) {
          toDenseVecMatrix().multiply(mat.toDenseVecMatrix(), cores)
        } else {
          //num of rows to be split of this matrix
          val mSplitNum = numBlksByRow()
          //num of columns to be split of this matrix, meanwhile num of rows of that matrix
          val kSplitNum = numBlksByCol()
          //num of columns to be split of that matrix
          val nSplitNum = mat.numBlksByCol()
          val partitioner = new HashPartitioner(2 * cores)

          if (mSplitNum == 1 && nSplitNum == 1) {
            val result = blocks.join(mat.blocks)
              .mapPartitions( {
              iter => iter.map {
                  t =>
                    val b1 = t._2._1.asInstanceOf[BDM[Double]]
                    val b2 = t._2._2.asInstanceOf[BDM[Double]]
                    val c = (b1 * b2).asInstanceOf[BDM[Double]]
                    (new BlockID (t._1.row, t._1.column), c)
                }
            }).partitionBy(partitioner).persist().reduceByKey(_ + _)
            new BlockMatrix(result, numRows(), mat.numCols(), mSplitNum, nSplitNum)
          } else {
            val thisEmitBlocks = blocks.mapPartitions( {
              iter =>
                iter.flatMap(t => {
                  val array = Array.ofDim[(BlockID, BDM[Double])](nSplitNum)
                  for (i <- 0 until nSplitNum) {
                    val seq = t._1.row * nSplitNum * kSplitNum + i * kSplitNum + t._1.column
                    array(i) = (new BlockID(t._1.row, i, seq), t._2)
                  }
                  array
                })
            }).partitionBy(partitioner).cache()
            val otherEmitBlocks = mat.blocks.mapPartitions( {
              iter =>
                iter.flatMap(t => {
                  val array = Array.ofDim[(BlockID, BDM[Double])](mSplitNum)
                  for (i <- 0 until mSplitNum) {
                    val seq = i * nSplitNum * kSplitNum + t._1.column * kSplitNum + t._1.row
                    array(i) = (new BlockID(i, t._1.column, seq), t._2)
                  }
                  array
                })
            }).partitionBy(partitioner).cache()

            if (kSplitNum != 1){
              val result = thisEmitBlocks.join(otherEmitBlocks)
                .mapPartitions({
                iter =>
                  iter.map{
                    t =>
                      val b1 = t._2._1.asInstanceOf[BDM[Double]]
                      val b2 = t._2._2.asInstanceOf[BDM[Double]]
                      val c = (b1 * b2).asInstanceOf[BDM[Double]]
                      (new BlockID(t._1.row, t._1.column), c)
                  }
              }).partitionBy(partitioner).persist().reduceByKey (_ + _)
              new BlockMatrix(result, numRows(), other.numCols (), mSplitNum, nSplitNum)
            } else {
              val result = thisEmitBlocks.join(otherEmitBlocks)
                .map(t => {
                val mat = (t._2._1.asInstanceOf[BDM[Double]] * t._2._2.asInstanceOf[BDM[Double]])
                  .asInstanceOf[BDM[Double]]
                (new BlockID(t._1.row, t._1.column), mat)
              })
              new BlockMatrix (result, numRows(), other.numCols(), mSplitNum, nSplitNum)
            }
          }
        }
      }
      case mat: DenseVecMatrix => {
        // if the other matrix is small, just broadcast it, it is beneficial when several matrices multiplication
        val broadSize = 300 * 1024 * 1024 / 8
        if (mat.numRows() * mat.numCols() < broadSize){
          val blockMatrix = mat.toBlockMatrix((mat.numRows() * numBlksByCol() / numCols()).toInt,
              (mat.numCols() * numBlksByRow() / numRows()).toInt)
       /*the following logic is problematic　as the two matrix multiplier's dimension mismatches
        *   val broadBDM = blocks.context.broadcast(blockMatrix)
         multiply()
         val result = blocks.mapPartitions( iter => {
            iter.map( t => {
              (t._1, (t._2 * broadBDM.value).asInstanceOf[BDM[Double]])
            })
          })
         new BlockMatrix(result, numRows(), mat.numCols(), numBlksByRow(), numBlksByCol())
         * */
         multiply(blockMatrix, cores)
        }else {
          toDenseVecMatrix().multiply(mat, cores)
        }
      }
    }
  }

  /**
   * element-wise multiply another number
   *
   * @param b the number to be multiplied
   * @return the result in BlockMatrix type
   */
  final def multiply(b: Double): BlockMatrix = {
    val result = blocks.mapValues(t => (t * b).asInstanceOf[BDM[Double]])
    new BlockMatrix(result, numRows(), numCols(), numBlksByRow(), numBlksByCol())
  }

  /**
   * add another matrix
   *
   * @param other the matrix to be added or the number to be added
   * @return the addition result in DenseVecMatrix or BlockMatrix type,
   *         depends on the structure of two input matrices
   */
  final def add(other: DistributedMatrix): DistributedMatrix = {
    other match {
      case mat: DenseVecMatrix => {
        require(numRows() == mat.numRows() &&
          numCols() == mat.numCols(), s"matrix dimension mismatch")
        toDenseVecMatrix().add(mat)}
      case mat: BlockMatrix => {
        require(numRows() == mat.numRows() &&
          numCols() == mat.numCols(), s"matrix dimension mismatch")
        if (numBlksByRow() != mat.numBlksByRow() || numBlksByCol() != mat.numBlksByCol()){
          toDenseVecMatrix().add(mat.toDenseVecMatrix())
        }else {
          val result = blocks.join(mat.blocks).mapValues(t => t._1 + t._2)
          new BlockMatrix(result, numRows(), numCols(), numBlksByRow(), numBlksByCol())
        }
      }
    }
  }

  /**
   * element-wise add another number
   *
   * @param b the number to be element-wise added
   * @return result in BlockMatrix type
   */
  final def add(b: Double): BlockMatrix = {
    val result = blocks.mapValues(t => (t + b).asInstanceOf[BDM[Double]])
    new BlockMatrix(result, numRows(), numCols(), numBlksByRow(), numBlksByCol())
  }


  /**
   * subtract another matrix
   *
   * @param other the matrix to be added or the number to be minus
   * @return the result in DistributedMatrix type, specific matrix type depends on the other matrix
   */
  final def subtract(other: DistributedMatrix): DistributedMatrix = {
    other match {
      case mat: DenseVecMatrix => {
        require(numRows() == mat.numRows() &&
          numCols() == mat.numCols(), s"matrix dimension mismatch")
        toDenseVecMatrix().subtract(mat)}
      case mat: BlockMatrix => {
        require(numRows() == mat.numRows() &&
          numCols() == mat.numCols(), s"matrix dimension mismatch")
        if (numBlksByRow() != mat.numBlksByRow() || numBlksByCol() != mat.numBlksByCol()){
          toDenseVecMatrix().subtract(mat.toDenseVecMatrix())
        }else {
          val result = blocks.join(mat.blocks).mapValues(t => t._1 - t._2)
          new BlockMatrix(result, numRows(), numCols(), numBlksByRow(), numBlksByCol())
        }
      }
    }
  }

  /**
   * element-wise subtract another number
   *
   * @param b the number to be element-wise subtracted
   * @return the result in BlockMatrix type
   */
  final def subtract(b: Double): BlockMatrix = {
    val result = blocks.mapValues(t => (t - b).asInstanceOf[BDM[Double]])
    new BlockMatrix(result, numRows(), numCols(), numBlksByRow(), numBlksByCol())
  }

  /**
   * Element in this matrix element-wise substract by another scalar
   *
   * @param b a number in the format of double
   */
  final def subtractBy(b: Double): BlockMatrix = {
    val result = blocks.mapValues(t => {
      val array = t.data
      for (i <- 0 until array.length){
        array(i)= b - array(i)
      }
      BDM.create[Double](t.rows, t.cols, array)
    })
    new BlockMatrix(result, numRows(), numCols(), numBlksByRow(), numBlksByCol())
  }

  /**
   * Element in this matrix element-wise divide another scalar
   *
   * @param b a number in the format of double
   * @return result in BlockMatrix type
   */
  final def divide(b: Double): BlockMatrix = {
    val result = blocks.mapValues(t => (t / b).asInstanceOf[BDM[Double]])
    new BlockMatrix(result, numRows(), numCols(), numBlksByRow(), numBlksByCol())
  }

  /**
   * Element in this matrix element-wise divided by another scalar
   *
   * @param b a number in the format of double
   */
  final def divideBy(b: Double): BlockMatrix = {
    val result = blocks.mapValues(t => {
      val array = t.data
      for (i <- 0 until array.length){
        array(i)= b / array(i)
      }
      BDM.create[Double](t.rows, t.cols, array)
    })
    new BlockMatrix(result, numRows(), numCols(), numBlksByRow(), numBlksByCol())
  }

  /**
   *  A transposed view of BlockMatrix
   *
   *  @return the transpose of this BlockMatrix
   */
  final def transpose(): BlockMatrix = {
    val result = blocks.mapPartitions( iter =>{
      iter.map ( t =>{
        (new BlockID(t._1.column, t._1.row), t._2.t )
      })
    })
    new BlockMatrix(result, numCols(), numRows(), numBlksByCol(), numBlksByRow())
  }

  /**
   * Using spark-property broadcast to decrease time used in the matrix-matrix multiplication
   *
   * @param other other matrix to be multiplied
   * @return the result matrix in BlockMatrix type
   */
  private[marlin] def multiplyBroadcast(other: BlockMatrix, parallelism: Int, splits:(Int, Int, Int), mode: String): BlockMatrix = {
    val tmp =  if (mode.toLowerCase.equals("broadcastb")) {
      val subBlocks = other.blocks.collect()
      val blocksArray =  Array.ofDim[BDM[Double]](other.blksByRow, other.blksByCol)
      for (s <- subBlocks) {
        blocksArray(s._1.row)(s._1.column) = s._2
      }
     val bArr = blocks.context.broadcast(blocksArray)

      blocks.mapPartitions(iter => {
        val res = Array.ofDim[(BlockID, BDM[Double])](other.blksByCol)
        val mats = bArr.value
        iter.flatMap(t => {
          for (j <- 0 until other.blksByCol) {
            res(j) = (new BlockID(t._1.row, j),
              (t._2 * mats(t._1.column)(j)).asInstanceOf[BDM[Double]])
          }
          res
        })
      })
    }else {
      val subBlocks = blocks.collect()
      val blocksArray = Array.ofDim[BDM[Double]](blksByRow, blksByRow)
      for (s <- subBlocks) {
        blocksArray(s._1.row)(s._1.column) = s._2
      }
      val bArr = blocks.context.broadcast(blocksArray)
      other.blocks.mapPartitions(iter => {
        val res = Array.ofDim[(BlockID, BDM[Double])](blksByRow)
        val mats = bArr.value
        iter.flatMap(t => {
          for (j <- 0 until blksByRow){
            res(j) = (new BlockID(j, t._1.column),
              (mats(j)(t._1.row) * t._2).asInstanceOf[BDM[Double]])
          }
          res
        })
      })
    }

    if (splits._2 == 1){
      new BlockMatrix(tmp, numRows(), other.numCols(),
        numBlksByRow(), other.numBlksByCol())
    }else {
      val result = tmp.partitionBy(new HashPartitioner(parallelism)).cache().reduceByKey(_ + _)
      new BlockMatrix(result, numRows(), other.numCols(),
        numBlksByRow(), other.numBlksByCol())
    }
  }

  /**
   * this function is used to save the martrix in DenseVecMatrix format
   * @param path the path to store in HDFS
   */
  def saveToFileSystem(path: String) {
    saveToFileSystem(path, " ")
  }
  
  /**
   * Save the result to the HDFS
   *
   * @param path the path to store in HDFS
   * @param format if set "blockmatrix", it will store in the format of [[edu.nju.pasalab.marlin.matrix.BlockMatrix]]
   *               and the data is in one-dimension column major array,
   *               else it will store in the format of [[edu.nju.pasalab.marlin.matrix.DenseVecMatrix]]
   */
  def saveToFileSystem(path: String, format: String = " "){
    if (format.toLowerCase.equals("blockmatrix")){
      blocks.map(t => (NullWritable.get(), new Text(t._1.row + "-" + t._1.column
        + "-" + t._2.rows + "-" + t._2.cols + ":" + t._2.data.mkString(","))))
        .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
    }else {
      toDenseVecMatrix().saveToFileSystem(path)
    }
  }

  /**
   * save the matrix in sequencefile in DenseVecMatrix format
   *
   * @param path the path to store in HDFS
   */
  def saveSequenceFile(path: String): Unit = {
    toDenseVecMatrix().saveSequenceFile(path)
  }

  /**
   * transform the BlockMatrix to DenseVecMatrix
   *
   * @return DenseVecMatrix with the same content
   */
  def toDenseVecMatrix(): DenseVecMatrix = {
    val mostBlockRowLen = math.ceil( numRows().toDouble / numBlksByRow().toDouble).toInt
    val mostBlockColLen = math.ceil( numCols().toDouble / numBlksByCol().toDouble).toInt
    //    blocks.cache()
    val result = blocks.flatMap( t => {
      val smRows = t._2.rows
      val smCols = t._2.cols
      val array = t._2.data
      val arrayBuf = Array.ofDim[(Long, (Int, Array[Double]))](smRows)
      for ( i <- 0 until smRows){
        val tmp = Array.ofDim[Double](smCols)
        for (j <- 0 until tmp.length){
          tmp(j) = array(j * smRows + i)
        }
        arrayBuf(i) = ( (t._1.row * mostBlockRowLen + i).toLong, (t._1.column, tmp) )
      }
      arrayBuf
    }).groupByKey()
      .map(input => {
      val array = Array.ofDim[Double](numCols().toInt)
      for (it <- input._2) {
        val colStart = mostBlockColLen * it._1
        for ( i <- 0 until it._2.length){
          array( colStart + i ) = it._2(i)
        }
      }
      (input._1 , Vectors.dense(array))
    })
    new DenseVecMatrix(result)
  }

  /**
   * Column bind to generate a new distributed matrix
   * @param other another matrix to be column bind
   * @return
   */
  def cBind(other: DistributedMatrix) : DistributedMatrix = {
    require( numRows() == other.numRows(), s"Row dimension mismatches: ${numRows()} vs ${other.numRows()}")
    other match {
      case that: BlockMatrix => {
        if (numBlksByRow() == that.numBlksByRow()){
          val result = that.blocks.map(t =>
            (new BlockID(t._1.row, t._1.column + numBlksByCol()), t._2)).union(blocks)
          new BlockMatrix(result, numRows(), numCols() + that.numCols(), blksByRow, blksByCol + that.blksByCol)
        }else {
          val thatDenVec = that.toDenseVecMatrix()
          val thisDenVec = this.toDenseVecMatrix()
          thisDenVec.cBind(thatDenVec)
        }
      }
      case that: DenseVecMatrix => {
        toDenseVecMatrix().cBind(that)
      }
      case  _ =>{
        throw new IllegalArgumentException("have not implemented yet")
      }
    }
  }

  /**
   * print the matrix out
   */
  def print() {
    if  (numBlksByRow() * numBlksByCol() > 4){
      blocks.take(4).foreach(t => println("blockID :[" + t._1.row + ", " + t._1.column
        + "], block content below:\n" + t._2.toString()))
      println("there are " + (numBlksByRow() * numBlksByCol() - 4) + " blocks more")
    }else {
      blocks.collect().foreach(t => println("blockID :[" + t._1.row + ", " + t._1.column
        + "], block content below:\n"+ t._2.toString()))
    }
  }

}


