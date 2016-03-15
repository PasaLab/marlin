package edu.nju.pasalab.marlin.matrix

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import edu.nju.pasalab.marlin.utils.MTUtils
import scala.collection.mutable.ArrayBuffer
import scala.{specialized => spec}
import edu.nju.pasalab.marlin.rdd.MatrixMultPartitioner
import org.apache.hadoop.io.{Text, NullWritable}
import org.apache.hadoop.mapred.TextOutputFormat

import org.apache.spark.{Partitioner, Logging, HashPartitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.util.Random


/**
 * BlockMatrix representing several [[breeze.linalg.DenseMatrix]] make up the matrix
 * with BlockID
 *
 * @param blocks blocks of this matrix
 * @param nRows number of rows
 * @param nCols number of columns
 * @param blksByRow block nums by row
 */

class BlockMatrix(private[marlin] val blocks: RDD[(BlockID, SubMatrix)], private var nRows: Long, private var nCols: Long, private var blksByRow: Int, private var blksByCol: Int) extends DistributedMatrix with Logging{


  /** Alternative constructor leaving matrix dimensions to be determined automatically. */
  def this(blocks: RDD[(BlockID, SubMatrix)]) = this(blocks, 0L, 0L, 0, 0)


  /** Gets or computes the number of rows. */
  override def numRows(): Long = {
    if (nRows <= 0L) {
      nRows = blocks.filter(_._1.column == 0).map(_._2.rows).reduce(_ + _)
    }
    nRows
  }

  /** Gets or computes the number of columns. */
  override def numCols(): Long = {
    if (nCols <= 0L) {
      nCols = blocks.filter(_._1.row == 0).map(_._2.cols).reduce(_ + _)
    }
    nCols
  }

  /** Gets or computes the number of blocks by the direction of row. */
  def numBlksByRow(): Int = {
    if (blksByRow <= 0L) {
      blksByRow = blocks.filter(_._1.column == 0).count().toInt
    }
    blksByRow
  }

  /** Gets or computes the number of blocks by the direction of column. */
  def numBlksByCol(): Int = {
    if (blksByCol <= 0L) {
      blksByCol = blocks.filter(_._1.row == 0).count().toInt
    }
    blksByCol
  }

  def getBlocks = blocks

  /** Collects data and assembles a local dense breeze matrix (for test only). */
  override private[matrix] def toBreeze(): BDM[Double] = {
    val m = numRows().toInt
    val n = numCols().toInt
    val mostBlkRowLen = math.ceil(m.toDouble / numBlksByRow().toDouble).toInt
    val mostBlkColLen = math.ceil(n.toDouble / numBlksByCol().toDouble).toInt
    val mat = BDM.zeros[Double](m, n)
    blocks.collect().foreach {
      case (blkID, matrix) =>
        val rowStart = blkID.row * mostBlkRowLen
        val colStart = blkID.column * mostBlkColLen
        // TODO support sparse format
        mat(rowStart until rowStart + matrix.rows,
          colStart until colStart + matrix.cols) := matrix.denseBlock
    }
    mat
  }

  def multiply(other: DistributedMatrix,
               cores: Int,
               broadcastThreshold: Int = 300): DistributedMatrix = {
    require(numCols == other.numRows(),
      s"Dimension mismatch during matrix-matrix multiplication: ${numCols()} vs ${other.numRows()}")
    other match {
      case that: DenseVecMatrix =>
        val broadcastSize = broadcastThreshold * 1024 * 1024 / 8
        if (that.numRows() * that.numCols() <= broadcastSize) {
          multiply(that.toBreeze())
        } else if (numRows() * numCols() <= broadcastSize) {
          that.multiply(this.toBreeze())
        } else if (0.8 < (numRows() * that.numCols()).toDouble / (numCols() * numCols()).toDouble
          && (numRows() * that.numCols()).toDouble / (numCols() * numCols()).toDouble < 1.2
          && numRows() / numCols() < 1.2
          && numRows() / numCols() > 0.8) {
          val split = math.floor(math.pow(3 * cores, 1.0 / 3.0)).toInt
          multiply(that, (split, split, split))
        } else {
          val splitMethod =
            MTUtils.splitMethod(numRows(), numCols(), other.numCols(), cores)
          multiply(that, splitMethod)
        }
      case that: BlockMatrix =>
        val broadSize = broadcastThreshold * 1024 * 1024 / 8
        if (that.numRows() * that.numCols() <= broadSize) {
          this.multiply(that.toBreeze())
        } else if(this.numRows() * this.numCols() <= broadSize ){
          that.multiplyBy(this.toBreeze())
        }else{
          val splitMethod =
            MTUtils.splitMethod(numRows(), numCols(), other.numCols(), cores)
          multiply(that, splitMethod)
        }
    }
  }

  /**
   * Given split mode, multiply two block matrices together
   *
   * @param other
   * @param splitMode represented as (m, k, n), which means this matrix would be split to (m,k) sub-blocks,
   *                   and other matrix would be split to (k,n) sub-blocks
   */
  def multiply(other: DistributedMatrix, splitMode: (Int, Int, Int)): BlockMatrix = {
    require(numCols() == other.numRows(), s"Dimension mismatch during " +
      s"matrix-matrix multiplication: ${numCols()} vs ${other.numRows()}")
    val (m, k, n) = splitMode
    other match {
      case that: BlockMatrix =>
        val matA = toBlockMatrix(m, k)
        val matB = that.toBlockMatrix(k, n)
        matA.multiply(matB)

      case that: DenseVecMatrix =>
        val matA = toBlockMatrix(m, k)
        val matB = that.toBlockMatrix(k, n)
        matA.multiply(matB)
    }

  }

  def multiply(other: BlockMatrix): BlockMatrix = {
    require(numCols() == other.numRows(), s"Dimension mismatch " +
      s"during matrix-matrix multiplication: ${numCols()} vs ${other.numRows()}")
    if (numBlksByCol() == other.numBlksByRow()) {
      //num of rows to be split of this matrix
      val mSplitNum = numBlksByRow()
      //num of columns to be split of this matrix, meanwhile num of rows of that matrix
      val kSplitNum = numBlksByCol()
      //num of columns to be split of that matrix
      val nSplitNum = other.numBlksByCol()
      val partitioner = new MatrixMultPartitioner(mSplitNum, kSplitNum, nSplitNum)

      val thisEmitBlocks = blocks.flatMap({ case(blkId, blk) =>
        Iterator.tabulate[(BlockID, SubMatrix)](nSplitNum)(i => {
          val seq = blkId.row * nSplitNum * kSplitNum + i * kSplitNum + blkId.column
          (BlockID(blkId.row, i, seq), blk)})
      }).partitionBy(partitioner)
      val otherEmitBlocks = other.blocks.flatMap({ case(blkId, blk) =>
        Iterator.tabulate[(BlockID, SubMatrix)](mSplitNum)(i => {
          val seq = i * nSplitNum * kSplitNum + blkId.column * kSplitNum + blkId.row
          (BlockID(i, blkId.column, seq), blk)
        })
      }).partitionBy(partitioner)
      if (kSplitNum != 1) {
        val result = thisEmitBlocks.join(otherEmitBlocks).mapPartitions(iter =>
          iter.map { case (blkId, (block1, block2)) =>
                (BlockID(blkId.row, blkId.column), block1.multiply(block2))
          }
        ).reduceByKey((a, b) => a.add(b))
        new BlockMatrix(result, numRows(), other.numCols(), mSplitNum, nSplitNum)
      } else {
        val result = thisEmitBlocks.join(otherEmitBlocks).mapPartitions(iter =>
          iter.map { case (blkId, (block1, block2)) =>
            (BlockID(blkId.row, blkId.column), block1.multiply(block2))
          }
        )
        new BlockMatrix(result, numRows(), other.numCols(), mSplitNum, nSplitNum)
      }
    } else if (numBlksByCol() % other.numBlksByRow() == 0) {
      if (numCols() % numBlksByCol() != 0) {
        throw new IllegalArgumentException("only supported BlockMatrix which all the sub-matrices have the same cols")
      }
      if (numCols() / numBlksByCol() % 2 != 0) {
        throw new IllegalArgumentException("only supported sub-matrices with even number cols")
      }
      val ratio = numBlksByCol() / other.numBlksByRow()
      val otherBlks = other.blocks.flatMap { case (blkId, mat) =>
        // TODO Update slice function
        Iterator.tabulate(ratio)(i => (new BlockID(blkId.row * ratio + i, blkId.column),
          new SubMatrix(denseMatrix = mat.denseBlock((i * mat.rows / ratio) to ((i + 1) * mat.rows / ratio - 1), ::))))
      }
      val otherSplit = new BlockMatrix(otherBlks)
      this.multiply(otherSplit)
    } else if (other.numBlksByRow() % numBlksByCol() == 0) {
      if (numCols() % numBlksByCol() != 0) {
        throw new IllegalArgumentException("only supported BlockMatrix which all the sub-matrices have the same cols")
      }
      if (numCols() / numBlksByCol() % 2 != 0) {
        throw new IllegalArgumentException("only supported sub-matrices with even number cols")
      }
      val ratio = other.numBlksByRow() / numBlksByCol()
      val thisBlks = blocks.flatMap { case (blkId, mat) =>
        // TODO Update slice function
      Iterator.tabulate(ratio)(i => (new BlockID(blkId.row * ratio + i, blkId.column),
        new SubMatrix(denseMatrix = mat.denseBlock((i * mat.rows / ratio) to ((i + 1) * mat.rows / ratio - 1), ::))))
      }
      val thisSplit = new BlockMatrix(thisBlks)
      thisSplit.multiply(other)
    } else {
      throw new IllegalArgumentException("currently not supported for the two dimension of matrices")
    }
  }


  /**
   * element-wise multiply another number
   *
   * @param b the number to be multiplied
   * @return the result in BlockMatrix type
   */
  def multiply(b: Double): BlockMatrix = {
    val result = blocks.mapValues(t => t.multiply(b))
    new BlockMatrix(result, numRows(), numCols(), numBlksByRow(), numBlksByCol())
  }


  /**
   * matrix- distributed vector multiplication using spark original join
   *
   * @param v
   */
  def multiply(v: DistributedVector): DistributedVector = {
    require(numCols() == v.length, s"Dimension mismatch " +
      s"during matrix-matrix multiplication ${numCols()} v.s ${v.length}")
    require(numBlksByCol() == v.splitNum, s"not supported matrix or vector")
    val m = numBlksByRow()
    val vectorEmits = v.vectors.flatMap { case (id, vector) =>
      Iterator.tabulate[(BlockID, DenseVector)](m)(i => (BlockID(i, id), vector))
    }
    if (numBlksByCol() != 1) {
      val vectors= blocks.join(vectorEmits).map { case (blkId, (mat, vec)) =>
        (blkId.row, mat.multiply(vec))
      }.reduceByKey((a, b) => (a.add(b)).asInstanceOf[DenseVector])
      new DistributedVector(vectors, v.length, v.splitNum)
    } else {
      val vectors = blocks.join(vectorEmits).map { case (blkId, (mat, vec)) =>
        (blkId.row, mat.multiply(vec))
      }
      new DistributedVector(vectors, v.length, v.splitNum)
    }
  }

  /**
   * matrix-vector multiplication
   * @param v other dense vector
   */
  def multiply(v: BDV[Double]): DistributedVector = {
    require(numCols() == v.length, s"matrix columns size ${numCols()} not support vector length ${v.length}")
    require(numBlksByCol() == 1, s"should not split the matrix by column")
    val vector = blocks.context.broadcast(v)
    val splits = blocks.mapPartitions(parts =>
      parts.map { case (blkID, blk) =>
        (blkID.row, blk.multiply(new DenseVector(vector.value)))
      })
    new DistributedVector(splits, this.numRows(), this.numBlksByRow())
  }

  /**
   * distributed block-matrix multiply a local small matrix
   * @param B
   */
  def multiply(B: BDM[Double]): BlockMatrix = {
    require(numCols() == B.rows, s"Dimension mismatch " +
      s"during matrix-matrix multiplication: ${numCols()} vs ${B.rows}")

    val Bb = getBlocks.context.broadcast(B)
    if (numBlksByCol() == 1) {
      val blocksMat = getBlocks.map{ case(blkId, blk) =>
        (blkId, (blk.multiply(Bb.value)))
      }
      new BlockMatrix(blocksMat, numRows(), B.cols, numBlksByRow(), numBlksByCol())
    }else {
      val colBlkSize = math.ceil(numCols().toDouble / numBlksByCol().toDouble).toInt
      val blocks = getBlocks.map { case(blkId, blk) =>
        val startRow = blkId.column * colBlkSize
        val endRow = if ((blkId.column + 1) * colBlkSize > numCols()) {
          numCols().toInt
        }else {
          (blkId.column + 1) * colBlkSize
        }
        (BlockID(blkId.row, 0), (blk.multiply(Bb.value(startRow until endRow, ::))))
      }.reduceByKey((a, b) => a.add(b))
      new BlockMatrix(blocks, numRows(), B.cols, numBlksByRow(), numBlksByCol())
    }
  }

  /**
   * a local small matrix multiply distributed block-matrix
   * @param B
   */
  private[marlin] def multiplyBy(B: BDM[Double]): BlockMatrix ={
    require(B.cols == numRows(), s"Dimension mismatch " +
      s"during matrix-matrix multiplication: ${B.cols} vs ${numRows()}")

    val Bb = getBlocks.context.broadcast(B)
    if (numBlksByRow() == 1) {
      val blocksMat = getBlocks.map{ case(blkId, blk) =>
        // TODO support matrix-matrix multiplication between BDM and SubMatrix
        (blkId, new SubMatrix(denseMatrix = (Bb.value * blk.denseBlock).asInstanceOf[BDM[Double]]))
      }
      new BlockMatrix(blocksMat, numRows(), B.cols, numBlksByRow(), numBlksByCol())
    }else {
      val rowBlkSize = math.ceil(numRows().toDouble / numBlksByRow().toDouble).toInt
      val blocks = getBlocks.map { case(blkId, blk) =>
        val startCol = blkId.row * rowBlkSize
        val endCol = if ((blkId.row + 1) * rowBlkSize > numCols()) {
          numCols().toInt
        }else {
          (blkId.row + 1) * rowBlkSize
        }
        // TODO support matrix-matrix multiplication between BDM and SubMatrix
        (blkId, new SubMatrix(denseMatrix = (Bb.value(::, startCol until endCol) *
          blk.denseBlock).asInstanceOf[BDM[Double]]))
      }.reduceByKey((a, b) => a.add(b))
      new BlockMatrix(blocks, B.rows, numCols(), numBlksByRow(), numBlksByCol())
    }
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
      case mat: DenseVecMatrix =>
        require(numRows() == mat.numRows() &&
          numCols() == mat.numCols(), s"matrix dimension mismatch")
        toDenseVecMatrix().add(mat)
      case mat: BlockMatrix =>
        require(numRows() == mat.numRows() &&
          numCols() == mat.numCols(), s"matrix dimension mismatch")
        if (numBlksByRow() != mat.numBlksByRow() || numBlksByCol() != mat.numBlksByCol()) {
          toDenseVecMatrix().add(mat.toDenseVecMatrix())
        } else {
          val result = blocks.join(mat.blocks).mapValues(t => t._1.add(t._2))
          new BlockMatrix(result, numRows(), numCols(), numBlksByRow(), numBlksByCol())
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
    val result = blocks.mapValues(t => (t.add(b)))
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
      case mat: DenseVecMatrix =>
        require(numRows() == mat.numRows() &&
          numCols() == mat.numCols(), s"matrix dimension mismatch")
        toDenseVecMatrix().subtract(mat)
      case mat: BlockMatrix =>
        require(numRows() == mat.numRows() &&
          numCols() == mat.numCols(), s"matrix dimension mismatch")
        if (numBlksByRow() != mat.numBlksByRow() || numBlksByCol() != mat.numBlksByCol()) {
          toDenseVecMatrix().subtract(mat.toDenseVecMatrix())
        } else {
          val result = blocks.join(mat.blocks).mapValues(t => t._1.subtract(t._2))
          new BlockMatrix(result, numRows(), numCols(), numBlksByRow(), numBlksByCol())
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
    val result = blocks.mapValues(t => t.subtract(b))
    new BlockMatrix(result, numRows(), numCols(), numBlksByRow(), numBlksByCol())
  }

  /**
   * Element in this matrix element-wise substract by another scalar
   *
   * @param b a number in the format of double
   */
  final def subtractBy(b: Double): BlockMatrix = {
    val result = blocks.mapValues(t => {
      val array = t.denseBlock.data
      for (i <- 0 until array.length) {
        array(i) = b - array(i)
      }
      // TODO support sparse format
      new SubMatrix(denseMatrix = BDM.create[Double](t.rows, t.cols, array))
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
    val result = blocks.mapValues(t => t.divide(b))
    new BlockMatrix(result, numRows(), numCols(), numBlksByRow(), numBlksByCol())
  }

  /**
   * Element in this matrix element-wise divided by another scalar
   *
   * @param b a number in the format of double
   */
  final def divideBy(b: Double): BlockMatrix = {
    val result = blocks.mapValues(t => {
      val array = t.denseBlock.data
      for (i <- 0 until array.length) {
        array(i) = b / array(i)
      }
      // TODO support sparse format
      new SubMatrix(denseMatrix =  BDM.create[Double](t.rows, t.cols, array))
    })
    new BlockMatrix(result, numRows(), numCols(), numBlksByRow(), numBlksByCol())
  }
//
//  def multiplySparse(other: BlockMatrix): CoordinateMatrix = {
//    blocks.flatMap{case(id, mat) =>
//        val arr = new ArrayBuffer[(Long, (Long, Float))]
//        for(sv <- mat.sparseBlock.values){
//
//        }
//    }
//  }

  /**
   * Sum all the elements in matrix ,note the Double.MaxValue is 1.7976931348623157E308
   *
   */
  def sum(): Double = {
    blocks.mapPartitions(iter => {
      // TODO support sparse format
      iter.map(t => t._2.denseBlock.data.sum)
    }, true).reduce(_ + _)
  }

  /**
   * count the sub-matrices of this BlockMatrix
   */
  def elementsCount(): Long = {
    blocks.count()
  }

  /**
   * Matrix-matrix dot product, the two input matrices must have the same row and column dimension
   * @param other the matrix to be dot product
   * @return
   */
  def dotProduct(other: DistributedMatrix): DistributedMatrix = {
    require(numRows() == other.numRows(), s"row dimension mismatch ${numRows()} vs ${other.numRows()}")
    require(numCols() == other.numCols(), s"column dimension mismatch ${numCols()} vs ${other.numCols()}")
    other match {
      case that: DenseVecMatrix =>
        toDenseVecMatrix().dotProduct(that)
      case that: BlockMatrix =>
        if (numBlksByRow() == that.numBlksByRow() && numBlksByCol() == that.numBlksByCol()) {
          val result = blocks.join(that.blocks).mapValues(t => {
            val rows = t._1.rows
            val cols = t._1.cols
            val array = t._1.denseBlock.data.zip(t._2.denseBlock.data)
              .map(x => x._1 * x._2)
            // TODO support sparse format
            new SubMatrix(denseMatrix = BDM.create[Double](rows, cols, array))
          })
          new BlockMatrix(result, numRows(), numCols(), numBlksByRow(), numBlksByCol())
        } else {
          toDenseVecMatrix().dotProduct(toDenseVecMatrix())
        }
    }
  }

  /**
   * A transposed view of BlockMatrix
   *
   * @return the transpose of this BlockMatrix
   */
  final def transpose(): BlockMatrix = {
    val result = blocks.mapPartitions(iter => {
      iter.map(x => {
        val mat: BDM[Double] = x._2.denseBlock.t.copy
        // TODO support sparse format
        (new BlockID(x._1.column, x._1.row), new SubMatrix(denseMatrix =  mat))
      })
    })
    new BlockMatrix(result, numCols(), numRows(), numBlksByCol(), numBlksByRow())
  }

  /**
   * This method still works in progress!
   * Get the inverse result of the matrix
   */
  def inverse(): BlockMatrix = {
    toDenseVecMatrix().inverse()
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
  def saveToFileSystem(path: String, format: String = " ") {
    if (format.toLowerCase.equals("blockmatrix")) {
      // TODO support sparse format
      blocks.map(t => (NullWritable.get(), new Text(t._1.row + "-" + t._1.column
        + "-" + t._2.rows + "-" + t._2.cols + ":" + t._2.denseBlock.data.mkString(","))))
        .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
    } else {
      toDenseVecMatrix().saveToFileSystem(path)
    }
  }

  //  /**
  //   * save the matrix in sequencefile in DenseVecMatrix format
  //   *
  //   * @param path the path to store in HDFS
  //   */
  //  def saveSequenceFile(path: String): Unit = {
  //    toDenseVecMatrix().saveSequenceFile(path)
  //  }

  /**
   * transform the BlockMatrix to DenseVecMatrix
   *
   * @return DenseVecMatrix with the same content
   */
  def toDenseVecMatrix(): DenseVecMatrix = {
    val mostBlockRowLen = math.ceil(numRows().toDouble / numBlksByRow().toDouble).toInt
    val mostBlockColLen = math.ceil(numCols().toDouble / numBlksByCol().toDouble).toInt
    // blocks.cache()
    val result = blocks.flatMap { case (id, blk) =>
      val smRows = blk.rows
      val smCols = blk.cols
      // TODO support sparse format
      Iterator.tabulate[(Long, (Int, BDV[Double]))](smRows)(i =>
        ((id.row * mostBlockRowLen + i).toLong, (id.column, blk.denseBlock(i, ::).t)))
    }.groupByKey().mapValues { iterable =>
      val iterator = iterable.iterator
      val vector = BDV.zeros[Double](numCols().toInt)
      for ((id, vec) <- iterator) {
        vector(mostBlockColLen * id until mostBlockColLen * id + vec.length) := vec
      }
      vector
    }
    new DenseVecMatrix(result)
  }

  def toDenseBlocks: BlockMatrix = {
    val result = blocks.mapValues{blk =>
      val mat = new SubMatrix(denseMatrix = blk.sparseBlock.toDense)
      blk.sparseBlock = null
      mat
    }
    new BlockMatrix(result, numRows, numCols, numBlksByRow(), numBlksByCol())
  }

  /**
   * transform a blockMatrix to another blockmatrix
   * @param newNumByRow
   * @param newNumByCol
   */
  def toBlockMatrix(newNumByRow: Int, newNumByCol: Int): BlockMatrix = {
    if (blksByRow == newNumByRow && blksByCol == newNumByCol) {
      this
    } else {
      val mostBlkRowLen = math.ceil(numRows().toDouble / numBlksByRow().toDouble).toInt
      val mostBlkColLen = math.ceil(numCols().toDouble / numBlksByCol().toDouble).toInt
      val newMostBlkRowLen = math.ceil(numRows().toDouble / newNumByRow.toDouble).toInt
      val newMostBlkColLen = math.ceil(numCols().toDouble / newNumByCol.toDouble).toInt
      val newBlksByRow = math.ceil(numRows().toDouble / newMostBlkRowLen).toInt
      val newBlksByCol = math.ceil(numCols().toDouble / newMostBlkColLen).toInt

      val splitByCol = (0 until numBlksByCol()).map(i =>
        (mostBlkColLen * i, math.min(mostBlkColLen * (i + 1) - 1, numCols().toInt - 1))).toArray
      val splitByRow = (0 until numBlksByRow()).map(i =>
        (mostBlkRowLen * i, math.min(mostBlkRowLen * (i + 1) - 1, numRows().toInt - 1))).toArray

      val splitStatusByCol = MTUtils.splitMethod(splitByCol, newMostBlkColLen)

      val splitStatusByRow = MTUtils.splitMethod(splitByRow, newMostBlkRowLen)

      val result = blocks.flatMap { case (blkId, blk) =>
        val row = blkId.row
        val col = blkId.column
        val rowSplits = splitStatusByRow(row).length
        val colSplits = splitStatusByCol(col).length
        val array = Array.ofDim[(BlockID, (Int, Int, Int, Int, BDM[Double]))](rowSplits * colSplits)
        var count = 0
        for ((rowId, (oldRow1, oldRow2), (newRow1, newRow2)) <- splitStatusByRow(row)) {
          for ((colId, (oldCol1, oldCol2), (newCol1, newCol2)) <- splitStatusByCol(col)) {
            // TODO support sparse format
            array(count) = (BlockID(rowId, colId), (newRow1, newRow2, newCol1, newCol2,
              blk.denseBlock(oldRow1 to oldRow2, oldCol1 to oldCol2).copy))
            count += 1
          }
        }
        array
      }.groupByKey().mapPartitions { iter =>
        iter.map { case (blkId, iterable) =>
          val rowLen = if ((blkId.row + 1) * newMostBlkRowLen > numRows()) {
            (numRows() - blkId.row * newMostBlkRowLen).toInt
          } else newMostBlkRowLen
          val colLen = if ((blkId.column + 1) * newMostBlkColLen > numCols()) {
            (numCols() - blkId.column * newMostBlkColLen).toInt
          } else newMostBlkColLen
          val mat = BDM.zeros[Double](rowLen, colLen)
          val iterator = iterable.iterator
          for ((rowStart, rowEnd, colStart, colEnd, blk) <- iterator) {
            mat(rowStart to rowEnd, colStart to colEnd) := blk
          }
          // TODO support sparse format
          (blkId, new SubMatrix(denseMatrix = mat))
        }
      }
      new BlockMatrix(result, numRows(), numCols(), newBlksByRow, newBlksByCol)
    }
  }


  /**
   * element-wise matrix-matrix multiplication
   * @param other
   * @param partitioner during join phase, with the customized partitioner, we can avoid the shuffle stage
   */
  def elementMultiply(other: BlockMatrix, partitioner: Partitioner): BlockMatrix = {
    val result = blocks.join(other.getBlocks, partitioner).map{ case(blkId, (blk1, blk2)) =>
        // TODO support sparse format
        blk1.denseBlock :*=  blk2.denseBlock
      (blkId, blk1)
    }
    new BlockMatrix(result, numRows(), numCols(), numBlksByRow(), numBlksByCol())
  }

  /**
   * Column bind to generate a new distributed matrix
   * @param other another matrix to be column bind
   * @return
   */
  def cBind(other: DistributedMatrix): DistributedMatrix = {
    require(numRows() == other.numRows(), s"Row dimension mismatches: ${numRows()} vs ${other.numRows()}")
    other match {
      case that: BlockMatrix =>
        if (numBlksByRow() == that.numBlksByRow()) {
          val result = that.blocks.map(t =>
            (new BlockID(t._1.row, t._1.column + numBlksByCol()), t._2)).union(blocks)
          new BlockMatrix(result, numRows(), numCols() + that.numCols(), blksByRow, blksByCol + that.blksByCol)
        } else {
          val thatDenVec = that.toDenseVecMatrix()
          val thisDenVec = this.toDenseVecMatrix()
          thisDenVec.cBind(thatDenVec)
        }
      case that: DenseVecMatrix =>
        toDenseVecMatrix().cBind(that)
      case _ =>
        throw new IllegalArgumentException("have not implemented yet")
    }
  }

  /**
   * Print the matrix out
   */
  def print() {
    if (numBlksByRow() * numBlksByCol() > 4) {
      blocks.take(4).foreach(t => println("blockID :[" + t._1.row + ", " + t._1.column
        + "], block content below:\n" + t._2.toString()))
      println("there are " + (numBlksByRow() * numBlksByCol()) + " blocks total...")
    } else {
      blocks.collect().foreach(t => println("blockID :[" + t._1.row + ", " + t._1.column
        + "], block content below:\n" + t._2.toString()))
    }
  }

  /**
   * Print the whole matrix out
   */
  def printAll() {
    blocks.collect().foreach(t => println("blockID :[" + t._1.row + ", " + t._1.column
      + "], block content below:\n" + t._2.toString()))
  }

}
