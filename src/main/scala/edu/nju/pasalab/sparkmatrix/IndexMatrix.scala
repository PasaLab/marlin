package edu.nju.pasalab.sparkmatrix

import java.io.IOException

import scala.collection.mutable.ArrayBuffer

import breeze.linalg.{DenseMatrix => BDM}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD


/**
 * This class overrides from [[org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix]]
 * Notice: some code in this file is copy from MLlib to make it compatible
 */
class IndexMatrix(
    val rows: RDD[IndexRow],
    private var nRows: Long,
    private var nCols: Long) extends DistributedMatrix{

  private var resultCols:Long = 0
  def this(rows: RDD[IndexRow]) = this(rows, 0L, 0)

  def this(sc: SparkContext , array: Array[Array[Double]] , partitions: Int = 2){
    this( sc.parallelize(array.zipWithIndex.
      map{ case(t,i)  => IndexRow(i, Vectors.dense(t)) }, partitions) )
  }

  override def numCols(): Long = {
    if (nCols <= 0) {
      // Calling `first` will throw an exception if `rows` is empty.
      nCols = rows.first().vector.size
    }
    nCols
  }

  override def numRows(): Long = {
    if (nRows <= 0L) {
      // Reduce will throw an exception if `rows` is empty.
      nRows = rows.map(_.index).reduce(math.max) + 1L
    }
    nRows
  }

  /** Collects data and assembles a local dense breeze matrix (for test only). */
  override private [sparkmatrix]  def toBreeze(): BDM[Double] = {
    val m = numRows().toInt
    val n = numCols().toInt
    val mat = BDM.zeros[Double](m, n)
    rows.collect().foreach { case IndexRow(rowIndex, vector) =>
      val i = rowIndex.toInt
      vector.toBreeze.activeIterator.foreach { case (j, v) =>
        mat(i, j) = v
      }
    }
    mat
  }

  /**
   * try to use CARMA, but currently we just change the split method
   *
   * @param other matrix to be multiplied, in the form of IndexMatrix
   * @param cores all the num of cores cross the cluster
   * @return
   */

  final def multiplyCarma(other: IndexMatrix, cores: Int): BlockMatrix = {
    val otherRows = other.numRows()
    require(this.numCols == otherRows, s"Dimension mismatch: ${this.numCols} vs ${otherRows}")
    val (mSplitNum, kSplitNum, nSplitNum) = MTUtils.splitMethod(this.numRows(), this.numCols(), other.numCols(), cores)
    val thisCollects = this.toBlockMatrix(mSplitNum, kSplitNum)
    val otherCollects = other.toBlockMatrix(kSplitNum, nSplitNum)
    thisCollects.multiply(otherCollects)

  }


  /**
   * A matrix multiply another IndexMatrix
   *
   * @param other another matrix in IndexMatrix format
   * @param blkNum is the split nums of submatries, if you set it as 10,
   *               which means you split every original large matrix into 10*10=100 blocks.
   *               The smaller this argument, the biger every worker get submatrix.
   *               When doing experiments, we multiply two 20000 by 20000 matrix together, we set it as 10.
   */
  final def multiply(other: IndexMatrix, blkNum: Int): BlockMatrix = {
    val otherRows = other.numRows()
    require(this.numCols == otherRows, s"Dimension mismatch: ${this.numCols} vs ${otherRows}")

    resultCols = other.numCols()

    val thisBlocks = this.toBlockMatrix(blkNum, blkNum)
    val otherBlocks = other.toBlockMatrix(blkNum, blkNum)
    thisBlocks.multiply(otherBlocks)
  }

  /**
   * This function is still in progress !
   * LU decompose this IndexMatrix to generate a lower triangular matrix L and a upper triangular matrix U
   *
   * @return a pair (lower triangular matrix, upper triangular matrix)
   */
  def luDecompose(mode: String = "auto"): (IndexMatrix, IndexMatrix) = {
    val iterations = this.numRows
    require(iterations == this.numCols, s"currently we only support square matrix: ${iterations} vs ${this.numCols}")

//    object LUmode extends Enumeration {
//      val LocalBreeze, DistSpark = Value
//    }
//    val computeMode =  mode match {
//      case "auto" => if ( iterations > 10000L){
//        LUmode.DistSpark
//      }else {
//        LUmode.LocalBreeze
//      }
//      case "breeze" => LUmode.LocalBreeze
//      case "dist" => LUmode.DistSpark
//      case _ => throw new IllegalArgumentException(s"Do not support mode $mode.")
//    }
//
//    val (lower: IndexMatrix, upper: IndexMatrix) = computeMode match {
//      case LUmode.LocalBreeze =>
//       val temp =  bLU(this.toBreeze())
//        Matrices.fromBreeze(breeze.linalg.lowerTriangular(temp._1))
//    }
//


    //copy construct a IndexMatrix to maintain the original matrix
    var matr = new IndexMatrix(rows.map( t => {
      val array = Array.ofDim[Double](numCols().toInt)
      val v = t.vector.toArray
      for ( k <- 0 until v.length){
        array(k) = v.apply(k)
      }
      new IndexRow(t.index, Vectors.dense(array))}))

    val num = iterations.toInt

    var lowerMat = new IndexMatrix( this.rows.map( t => new IndexRow(t.index , Vectors.sparse(t.vector.size , Seq()))) )

    for (i <- 0 until num) {
     val vector = matr.rows.filter(t => t.index.toInt == i).map(t => t.vector).first()
     val c = matr.rows.context.broadcast(vector.apply(i))
     val broadVec = matr.rows.context.broadcast(vector)

      //TODO: here we omit the compution of L

      //TODO: here collect() is too much cost, find another method
      val lupdate = matr.rows.map( t => (t.index , t.vector.toArray.apply(i) / c.value)).collect()
      val updateVec = Array.ofDim[Double](num)
      for ( l <- lupdate){
        updateVec.update(l._1.toInt , l._2)
      }

      val broadLV = matr.rows.context.broadcast(updateVec)

      val lresult = lowerMat.rows.map( t => {
        if ( t.index.toInt >= i) {
          val vec = t.vector.toArray
          vec.update(i, broadLV.value.apply(t.index.toInt))
          new IndexRow(t.index, Vectors.dense(vec))
        }else t
      })
      lowerMat = new IndexMatrix(lresult)
      //cache the lower matrix to speed the compution
      lowerMat.rows.cache()
         val result = matr.rows.map(t => {
         if ( t.index.toInt > i){
          val vec = t.vector.toArray
          val lupdate = vec.apply(i)/c.value
          val mfactor = -vec.apply(i) / c.value
          for (k <- 0 until vec.length) {
            vec.update(k, vec.apply(k) + mfactor * broadVec.value.apply(k))
          }
           new IndexRow(t.index, Vectors.dense(vec))
         }
         else t
        })
      matr = new IndexMatrix(result)

      //cache the matrix to speed the compution
      matr.rows.cache()
      if (i % 400 == 0)
        matr.rows.checkpoint()
    }

    (lowerMat, matr)
  }


  /**
   * This matrix add another IndexMatrix
   *
   * @param other another matrix in IndexMatrix format
   */
  final def add(other: IndexMatrix): IndexMatrix = {
    val nRows = this.numRows()
    val otherRows = other.numRows()
    require(nRows == otherRows, s"Dimension mismatch: ${nRows} vs ${otherRows}")
    require(this.numCols == other.numCols, s"Dimension mismatch: ${this.numCols} vs ${other.numCols}")

    val result = this.rows
      .map(t => (t.index, t.vector))
      .join(other.rows.map(t => (t.index, t.vector)))
      .map(t => IndexRow(t._1, Vectors.fromBreeze(t._2._1.toBreeze + t._2._2.toBreeze)))
    new IndexMatrix(result)
  }

  /**
   * This matrix minus another IndexMatrix
   *
   * @param other another matrix in IndexMatrix format
   */
  final def minus(other: IndexMatrix): IndexMatrix = {
    val nRows = this.numRows()
    val otherRows = other.numRows()
    require(nRows == otherRows, s"Dimension mismatch: ${nRows} vs ${otherRows}")
    require(this.numCols == other.numCols, s"Dimension mismatch: ${this.numCols} vs ${other.numCols()}")

    val result = this.rows
      .map(t => (t.index, t.vector))
      .join(other.rows.map(t => (t.index, t.vector)))
      .map(t => IndexRow(t._1, Vectors.fromBreeze(t._2._1.toBreeze - t._2._2.toBreeze)))
    new IndexMatrix(result)
  }


  /**
   * Element in this matrix element-wise add another scalar
   *
   * @param b a number in the format of double
   */
  final def elemWiseAdd(b: Double): IndexMatrix = {
    val result = this.rows.map(t =>IndexRow(t.index, Vectors.dense(t.vector.toArray.map(_ + b))))
    new IndexMatrix(result)
  }

  /**
   * Element in this matrix element-wise minus another scalar
   *
   * @param b a number in the format of double
   */
  final def elemWiseMinus(b: Double): IndexMatrix = {
    val result = this.rows.map(t =>IndexRow(t.index, Vectors.dense(t.vector.toArray.map(_ - b))))
    new IndexMatrix(result)
  }

  /**
   * Element in this matrix element-wise minus by another scalar
   *
   * @param b a number in the format of double
   */
  final def elemWiseMinusBy(b: Double): IndexMatrix = {
    val result = this.rows.map(t =>IndexRow(t.index, Vectors.dense(t.vector.toArray.map(b - _ ))))
    new IndexMatrix(result)
  }

  /**
   * Element in this matrix element-wise multiply another scalar
   *
   * @param b a number in the format of double
   */
  final def elemWiseMult(b: Double): IndexMatrix = {
    val result = this.rows.map(t =>IndexRow(t.index, Vectors.dense(t.vector.toArray.map(_ * b))))
    new IndexMatrix(result)
  }

  /**
   * Element in this matrix element-wise divide another scalar
   *
   * @param b a number in the format of double
   */
  final def elemWiseDivide(b: Double): IndexMatrix = {
    val result = this.rows.map(t =>IndexRow(t.index, Vectors.dense(t.vector.toArray.map( _ / b))))
    new IndexMatrix(result)
  }

  /**
   * Element in this matrix element-wise divided by another scalar
   *
   * @param b a number in the format of double
   */
  final def elemWiseDivideBy(b: Double): IndexMatrix = {
    val result = this.rows.map(t =>IndexRow(t.index, Vectors.dense(t.vector.toArray.map( b / _))))
    new IndexMatrix(result)
  }

  /**
   * Get sub matrix according to the given range of rows
   *
   * @param startRow the start row of the subMatrix, this row is included
   * @param endRow the end row of the subMatrix, this row is included
   */
  final def sliceByRow(startRow: Long, endRow: Long): IndexMatrix = {
    require((startRow >= 0 && endRow <= this.numRows()), s"start row or end row dismatch the matrix num of rows")
    new IndexMatrix(this.rows.filter(t => (t.index >= startRow && t.index <= endRow)))
  }

  /**
   * get sub matrix according to the given range of column
   *
   * @param startCol the start column of the subMatrix, this column is included
   * @param endCol the end column of the subMatrix, this column is included
   */
  final def sliceByColumn(startCol: Int, endCol: Int): IndexMatrix = {
    require((startCol >= 0 && endCol <= this.numCols()),
      s"start column or end column dismatch the matrix num of columns")

    new IndexMatrix(this.rows.map(t => IndexRow(t.index, Vectors.dense(t.vector.toArray.slice(startCol, endCol+1)))))
  }

  /**
   * get sub matrix according to the given range of column
   *
   * @param startRow the start row of the subMatrix, this row is included
   * @param endRow the end row of the subMatrix, this row is included
   * @param startCol the start column of the subMatrix, this column is included
   * @param endCol the end column of the subMatrix, this column is included
   */
  final def getSubMatrix(startRow: Long, endRow: Long ,startCol: Int, endCol: Int): IndexMatrix = {
    require((startRow >= 0 && endRow <= this.numRows()), s"start row or end row dismatch the matrix num of rows")
    require((startCol >= 0 && endCol <= this.numCols()),
      s"start column or end column dismatch the matrix num of columns")

    new IndexMatrix(this.rows
      .filter(t => (t.index >= startRow && t.index <= endRow))
      .map(t => IndexRow(t.index, Vectors.dense(t.vector.toArray.slice(startCol,endCol+1)))))
  }



  /**
   * Save the result to the HDFS
   *
   * @param path the path to store the IndexMatrix in HDFS
   */
  def saveToFileSystem(path: String){
    this.rows.saveAsTextFile(path)
  }

//  /**
//   * function used in multiply
//   */
//  private [sparkmatrix] def collectTable(matrix: IndexMatrix, blckkNum: Int, matrixPos: Boolean)
//  :RDD[(BlockID, BDM[Double])] = {
//    //val log = Logger.getLogger(this.getClass)
//    val mRows = matrix.numRows().toInt
//    val mColumns = matrix.numCols().toInt
//    val mBlockRowSize = math.ceil(mRows.toDouble / blckkNum.toDouble).toInt
//    val mBlockColSize = math.ceil(mColumns.toDouble / blckkNum.toDouble).toInt
//    //get subVector according to34 the column size and is keyed by the blockID
//    matrix.rows.flatMap( t =>{
//        var startColumn = 0
//        var endColumn = 0
//        var arrayBuf= new ArrayBuffer[(BlockID, IndexRow)]
//
//        val elems = t.vector.toArray
//        var i = 0
//        while(endColumn < (mColumns -1)) {
//          startColumn = i * mBlockColSize
//          endColumn = startColumn + mBlockColSize - 1
//          if (endColumn >= mColumns) {
//            endColumn = mColumns - 1
//          }
//
//          val vector = new Array[Double](endColumn - startColumn + 1)
//          for (j <- startColumn to endColumn) {
//            vector(j - startColumn) = elems(j)
//          }
//
//          arrayBuf += ((new BlockID(t.index.toInt / mBlockRowSize, i),new IndexRow(t.index,Vectors.dense(vector))))
//          i += 1
//        }
//        arrayBuf})
//      .groupByKey()
//      //emit the block according to the new BlockID
//      .flatMap(input => {
//        val colBase = input._1.column * mBlockColSize
//        val rowBase = input._1.row * mBlockRowSize
//
//        //the block's size: rows & columns
//        var smRows = mBlockRowSize
//        if ((rowBase + mBlockRowSize - 1) >= mRows) {
//          smRows = mRows - rowBase
//        }
//
//        var smCols = mBlockColSize
//        if ((colBase + mBlockColSize - 1) >= mColumns) {
//           smCols = mColumns - colBase
//        }
//
//        val itr = input._2.iterator
//        //to generate the local matrix, be careful, the array is column major
//        val array = Array.ofDim[Double](smRows * smCols)
//
//        while (itr.hasNext) {
//          val vec = itr.next()
//          if (vec.vector.size != smCols) {
//            Logger.getLogger(this.getClass).log(Level.ERROR,"vectors:  "+ input._2+"Block Column Size dismatched" )
//            throw new IOException("Block Column Size dismatched")
//          }
//
//          val rowOffset = vec.index.toInt - rowBase
//          if (rowOffset >= smRows || rowOffset < 0) {
//            Logger.getLogger(this.getClass).log(Level.ERROR,"Block Row Size dismatched" )
//            throw new IOException("Block Row Size dismatched")
//          }
//
//          val tmp = vec.vector.toArray
//          for (i <- 0 until tmp.length) {
//            array(i * smRows + rowOffset) = tmp(i)
//          }
//        }
//
//        val subMatrix = new BDM(smRows, smCols, array)
//        var arrayBuf = new ArrayBuffer[(BlockID , BDM[Double])]
//        if (matrixPos) {
//          for (x <- 0 to blckkNum - 1) {
//            val r: Int = input._1.row * blckkNum * blckkNum
//            val seq: Int = x * blckkNum + input._1.column + r
//            arrayBuf += ((new BlockID( input._1.row, x, seq), subMatrix))
//          }
//        }else{
//          for (x <- 0 to blckkNum - 1) {
//            val seq = x * blckkNum * blckkNum + input._1.column * blckkNum + input._1.row
//            arrayBuf += ((new BlockID(x, input._1.column, seq), subMatrix))
//          }
//        }
//        arrayBuf
//    })
//  }

  /**
   * transform the IndexMatrix to BlockMatrix
   *
   * @param numByRow num of subMatrix by row
   * @param numByCol num of subMatrix by column
   * @return
   */

  def toBlockMatrix(numByRow: Int, numByCol: Int): BlockMatrix = {
    val mRows = this.numRows().toInt
    val mColumns = this.numCols().toInt
    val mBlockRowSize = math.ceil(mRows.toDouble / numByRow.toDouble).toInt
    val mBlockColSize = math.ceil(mColumns.toDouble / numByCol.toDouble).toInt
    val result = rows.flatMap( t => {
      var startColumn = 0
      var endColumn = 0
      var arrayBuf= new ArrayBuffer[(BlockID, IndexRow)]

      val elems = t.vector.toArray
      var i = 0
      while(endColumn < (mColumns -1)) {
        startColumn = i * mBlockColSize
        endColumn = startColumn + mBlockColSize - 1
        if (endColumn >= mColumns) {
          endColumn = mColumns - 1
        }

        val vector = new Array[Double](endColumn - startColumn + 1)
        for (j <- startColumn to endColumn) {
          vector(j - startColumn) = elems(j)
        }

        arrayBuf += ((new BlockID(t.index.toInt / mBlockRowSize, i),new IndexRow(t.index,Vectors.dense(vector))))
        i += 1
      }
      arrayBuf
    })
      .groupByKey()
      .map(input => {
      val colBase = input._1.column * mBlockColSize
      val rowBase = input._1.row * mBlockRowSize

      //the block's size: rows & columns
      var smRows = mBlockRowSize
      if ((rowBase + mBlockRowSize - 1) >= mRows) {
        smRows = mRows - rowBase
      }

      var smCols = mBlockColSize
      if ((colBase + mBlockColSize - 1) >= mColumns) {
        smCols = mColumns - colBase
      }

      val itr = input._2.iterator
      //to generate the local matrix, be careful, the array is column major
      val array = Array.ofDim[Double](smRows * smCols)

      while (itr.hasNext) {
        val vec = itr.next()
        if (vec.vector.size != smCols) {
          Logger.getLogger(this.getClass).log(Level.ERROR,"vectors:  "+ input._2+"Block Column Size dismatched" )
          throw new IOException("Block Column Size dismatched")
        }

        val rowOffset = vec.index.toInt - rowBase
        if (rowOffset >= smRows || rowOffset < 0) {
          Logger.getLogger(this.getClass).log(Level.ERROR,"Block Row Size dismatched" )
          throw new IOException("Block Row Size dismatched")
        }

        val tmp = vec.vector.toArray
        for (i <- 0 until tmp.length) {
          array(i * smRows + rowOffset) = tmp(i)
        }
      }

      val subMatrix = new BDM(smRows, smCols, array)
      new Block(input._1, subMatrix)
    })

    new BlockMatrix(result, numRows(), numCols(), numByRow, numByCol)
  }

}
