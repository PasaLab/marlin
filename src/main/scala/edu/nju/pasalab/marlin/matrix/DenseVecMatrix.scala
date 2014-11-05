package edu.nju.pasalab.marlin.matrix

import java.io.IOException

import scala.collection.mutable.ArrayBuffer

import breeze.linalg.{DenseMatrix => BDM}
import org.apache.log4j.{Logger, Level}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import edu.nju.pasalab.marlin.utils.MTUtils

import scala.collection.parallel.mutable.ParArray

/**
 * This class overrides from [[org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix]]
 * Notice: some code in this file is copy from MLlib to make it compatible
 */
class DenseVecMatrix(
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
  override private [matrix]  def toBreeze(): BDM[Double] = {
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
   *
   * @param other another matrix in DenseVecMatrix type
   * @param cores the real num of cores you set in the environment
   * @param broadcastThreshold the threshold of broadcasting variable, default num is 300 MB,
   *                           user can set it, the unit of this parameter is MB
   * @return result in BlockMatrix type
   */

  final def multiply(other: DenseVecMatrix,
            cores: Int,
            broadcastThreshold: Int = 300): BlockMatrix = {
    require(numCols == other.numRows(),
      s"Dimension mismatch: ${numCols} vs ${other.numRows()}")

    val broadSize = broadcastThreshold * 1024 * 1024 / 8
    if (other.numRows() * other.numCols() <= broadSize){
      val parallelism = math.min(8 * cores, numRows() / 2).toInt
      multiplyBroadcast(other, parallelism, (parallelism, 1, 1), "broadcastB" )
    }else if ( numRows() * numCols() <= broadSize ){
      val parallelism = math.min(8 * cores, other.numRows() / 2).toInt
      multiplyBroadcast(other, parallelism, (1, 1, parallelism), "broadcastA" )
    }else if (0.8 < (numRows() * other.numCols()).toDouble / (numCols() * numCols()).toDouble
      && (numRows() * other.numCols()).toDouble / (numCols() * numCols()).toDouble < 1.2
      && numRows() / numCols() < 1.2
      && numRows() / numCols() > 0.8){
      multiplyHama(other,math.floor(math.pow(3 * cores, 1.0/3.0 )).toInt)
    }else {
      multiplyCarma(other, cores)
    }
  }

  /**
   * A matrix multiply another DenseVecMatrix
   *
   * @param other another matrix in DenseVecMatrix format
   * @param blkNum is the split nums of submatries, if you set it as 10,
   *               which means you split every original large matrix into 10*10=100 blocks.
   *               The smaller this argument, the biger every worker get submatrix.
   * @return a distributed matrix in BlockMatrix type
   */
  final def multiplyHama(other: DenseVecMatrix, blkNum: Int): BlockMatrix = {
    val otherRows = other.numRows()
    require(numCols == otherRows, s"Dimension mismatch: ${numCols} vs ${otherRows}")

    resultCols = other.numCols()

    val thisBlocks = toBlockMatrix(blkNum, blkNum)
    val otherBlocks = other.toBlockMatrix(blkNum, blkNum)
    thisBlocks.multiply(otherBlocks, blkNum * blkNum * blkNum)
  }


  /**
   * refer to CARMA, implement the dimension-split ways
   *
   * @param other matrix to be multiplied, in the form of DenseVecMatrix
   * @param cores all the num of cores cross the cluster
   * @return a distributed matrix in BlockMatrix type
   */

  final def multiplyCarma(other: DenseVecMatrix, cores: Int): BlockMatrix = {
    val otherRows = other.numRows()
    require(numCols == otherRows, s"Dimension mismatch: ${numCols} vs ${otherRows}")
    val (mSplitNum, kSplitNum, nSplitNum) =
      MTUtils.splitMethod(numRows(), numCols(), other.numCols(), cores)
    val thisCollects = toBlockMatrix(mSplitNum, kSplitNum)
    val otherCollects = other.toBlockMatrix(kSplitNum, nSplitNum)
    thisCollects.multiply(otherCollects, cores)
  }


  /**
   * refer to CARMA, implement the dimension-split ways
   *
   * @param other matrix to be multiplied, in the form of DenseVecMatrix
   * @param parallelism all the num of cores cross the cluster
   * @param mode whether broadcast A or B
   * @return
   */

  final def multiplyBroadcast(other: DenseVecMatrix,
                              parallelism: Int,
                              splits:(Int, Int, Int), mode: String): BlockMatrix = {
    val otherRows = other.numRows()
    require(numCols == otherRows, s"Dimension mismatch: ${numCols} vs ${otherRows}")
    val thisCollects = toBlockMatrix(splits._1, splits._2)
    val otherCollects = other.toBlockMatrix(splits._2, splits._3)
    thisCollects.multiplyBroadcast(otherCollects, parallelism, splits, mode)
  }


  /**
   * This function is still in progress !
   * LU decompose this DenseVecMatrix to generate a lower triangular matrix L
   * and a upper triangular matrix U
   *
   * @return a pair (lower triangular matrix, upper triangular matrix)
   */
  def luDecompose(mode: String = "auto"): (DenseVecMatrix, DenseVecMatrix) = {
    val iterations = numRows
    require(iterations == numCols,
      s"currently we only support square matrix: ${iterations} vs ${numCols}")

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
//       val temp =  bLU(toBreeze())
//        Matrices.fromBreeze(breeze.linalg.lowerTriangular(temp._1))
//    }
//
    //copy construct a IndexMatrix to maintain the original matrix
    val matr = new DenseVecMatrix(rows.map( t => {
      val array = Array.ofDim[Double](numCols().toInt)
      val v = t.vector.toArray
      for ( k <- 0 until v.length){
        array(k) = v.apply(k)
      }
      new IndexRow(t.index, Vectors.dense(array))}))

    val num = iterations.toInt

    val lowerMat = new DenseVecMatrix( rows.map( t => new IndexRow(t.index , Vectors.sparse(t.vector.size , Seq()))) )

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

      lowerMat.rows.mapPartitions( iter => {
        iter.map { t =>
        if ( t.index.toInt >= i) {
          val vec = t.vector.toArray
          vec.update(i, broadLV.value.apply(t.index.toInt))
          new IndexRow(t.index, Vectors.dense(vec))
        }else t
      }}, true)
      
      //cache the lower matrix to speed the compution
         matr.rows.mapPartitions( iter =>{
            iter.map(t => {
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
        })}, true)

      //cache the matrix to speed the compution
      matr.rows.cache()
      if (i % 2000 == 0)
        matr.rows.checkpoint()
    }

    (lowerMat, matr)
  }


  /**
   * This matrix add another DenseVecMatrix
   *
   * @param other another matrix in DenseVecMatrix format
   */
  final def add(other: DenseVecMatrix): DenseVecMatrix = {
    val nRows = numRows()
    val otherRows = other.numRows()
    require(nRows == otherRows, s"Dimension mismatch: ${nRows} vs ${otherRows}")
    require(numCols == other.numCols, s"Dimension mismatch: ${numCols} vs ${other.numCols}")

    val result = rows
      .map(t => (t.index, t.vector))
      .join(other.rows.map(t => (t.index, t.vector)))
      .map(t => IndexRow(t._1, Vectors.fromBreeze(t._2._1.toBreeze + t._2._2.toBreeze)))
    new DenseVecMatrix(result, numRows(), numCols())
  }

  /**
   * This matrix minus another DenseVecMatrix
   *
   * @param other another matrix in DenseVecMatrix format
   */
  final def subtract(other: DenseVecMatrix): DenseVecMatrix = {
    require(numRows() == other.numRows(), s"Dimension mismatch: ${numRows()} vs ${other.numRows()}")
    require(numCols == other.numCols, s"Dimension mismatch: ${numCols} vs ${other.numCols()}")

    val result = rows
      .map(t => (t.index, t.vector))
      .join(other.rows.map(t => (t.index, t.vector)))
      .map(t => IndexRow(t._1, Vectors.fromBreeze(t._2._1.toBreeze - t._2._2.toBreeze)))
    new DenseVecMatrix(result, numRows(), numCols())
  }


  /**
   * Element in this matrix element-wise add another scalar
   *
   * @param b the number to be element-wise added
   */
  final def add(b: Double): DenseVecMatrix = {
    val result = rows.map(t =>IndexRow(t.index, Vectors.dense(t.vector.toArray.map(_ + b))))
    new DenseVecMatrix(result, numRows(), numCols())
  }

  /**
   * Element in this matrix element-wise minus another scalar
   *
   * @param b a number to be element-wise subtracted
   */
  final def subtract(b: Double): DenseVecMatrix = {
    val result = rows.map(t =>IndexRow(t.index, Vectors.dense(t.vector.toArray.map(_ - b))))
    new DenseVecMatrix(result, numRows(), numCols())
  }

  /**
   * Element in this matrix element-wise minus by another scalar
   *
   * @param b a number in the format of double
   */
  final def substactBy(b: Double): DenseVecMatrix = {
    val result = rows.map(t =>IndexRow(t.index, Vectors.dense(t.vector.toArray.map(b - _ ))))
    new DenseVecMatrix(result, numRows(), numCols())
  }

  /**
   * Element in this matrix element-wise multiply another scalar
   *
   * @param b a number in the format of double
   */
  final def multiply(b: Double): DenseVecMatrix = {
    val result = rows.map(t =>IndexRow(t.index, Vectors.dense(t.vector.toArray.map(_ * b))))
    new DenseVecMatrix(result, numRows(), numCols())
  }

  /**
   * Element in this matrix element-wise divide another scalar
   *
   * @param b a number in the format of double
   * @return result in DenseVecMatrix type
   */
  final def divide(b: Double): DenseVecMatrix = {
    val result = rows.map(t =>IndexRow(t.index, Vectors.dense(t.vector.toArray.map( _ / b))))
    new DenseVecMatrix(result, numRows(), numCols())
  }

  /**
   * Element in this matrix element-wise divided by another scalar
   *
   * @param b a number in the format of double
   */
  final def divideBy(b: Double): DenseVecMatrix = {
    val result = rows.map(t =>IndexRow(t.index, Vectors.dense(t.vector.toArray.map( b / _))))
    new DenseVecMatrix(result, numRows(), numCols())
  }

  /**
   * Get sub matrix according to the given range of rows
   *
   * @param startRow the start row of the subMatrix, this row is included
   * @param endRow the end row of the subMatrix, this row is included
   */
  final def sliceByRow(startRow: Long, endRow: Long): DenseVecMatrix = {
    require((startRow >= 0 && endRow <= numRows() && startRow <= endRow),
      s"start row or end row mismatch the matrix num of rows")

    new DenseVecMatrix(rows.filter(t => (t.index >= startRow && t.index <= endRow))
      , endRow - startRow + 1, numCols())
  }

  /**
   * get sub matrix according to the given range of column
   *
   * @param startCol the start column of the subMatrix, this column is included
   * @param endCol the end column of the subMatrix, this column is included
   */
  final def sliceByColumn(startCol: Int, endCol: Int): DenseVecMatrix = {
    require((startCol >= 0 && endCol <= numCols() && startCol <= endCol),
      s"start column or end column mismatch the matrix num of columns")

    new DenseVecMatrix(rows.map(t => IndexRow(t.index, Vectors.dense(t.vector.toArray.slice(startCol, endCol + 1))))
      , numRows(), endCol - startCol + 1)
  }

  /**
   * get sub matrix according to the given range of column
   *
   * @param startRow the start row of the subMatrix, this row is included
   * @param endRow the end row of the subMatrix, this row is included
   * @param startCol the start column of the subMatrix, this column is included
   * @param endCol the end column of the subMatrix, this column is included
   */
  final def getSubMatrix(startRow: Long, endRow: Long ,startCol: Int, endCol: Int): DenseVecMatrix = {
    require((startRow >= 0 && endRow <= numRows()), s"start row or end row dismatch the matrix num of rows")
    require((startCol >= 0 && endCol <= numCols()),
      s"start column or end column dismatch the matrix num of columns")

    new DenseVecMatrix(rows
      .filter(t => (t.index >= startRow && t.index <= endRow))
      .map(t => IndexRow(t.index, Vectors.dense(t.vector.toArray.slice(startCol,endCol + 1))))
    , endRow - startRow + 1, endCol - startCol + 1)
  }

  /**
   * compute the norm of this matrix
   *
   * @param mode the same with Matlab operations,
   *             `1` means get the 1-norm, the largest column sum of matrix
   *             `2` means get the largest singular value, the default mode --- need to do
   *             `inf` means the infinity norm, the largest row sum of matrix
   *             `fro` means the Frobenius-norm of matrix -- need to do
   */
  final def norm(mode: String): Double = {
    mode match {
      case "1" => {
        rows.mapPartitions( iter =>{
          val columnAcc = new ParArray[Double](numCols().toInt)
          iter.map( t => {
            val arr = t.vector.toArray
            for ( i <- 0 until arr.length){
              columnAcc(i) += math.abs(arr(i))
            }
          }
         )
        columnAcc.toIterator}).max()
      }
//      case "2" => {
//
//      }
      case "inf" => {
        rows.map( t => t.vector.toArray.reduce( _ + _ )).max()
      }
//      case "for" => {
//
//      }
    }
  }


  /**
   * Save the result to the HDFS
   *
   * @param path the path to store the DenseVecMatrix in HDFS or local file system
   */
  def saveToFileSystem(path: String){
    rows.saveAsTextFile(path)
  }

  /**
   * transform the DenseVecMatrix to BlockMatrix
   *
   * @param numByRow num of subMatrix along the row side set by user,
   *                 the actual num of subMatrix along the row side is `blksByRow`
   * @param numByCol num of subMatrix along the column side set by user,
   *                 the actual num of subMatrix along the row side is `blksByCol`
   * @return the transformated matrix in BlockMatrix type
   */

  def toBlockMatrix(numByRow: Int, numByCol: Int): BlockMatrix = {
    val mRows = numRows().toInt
    val mColumns = numCols().toInt
    val mBlockRowSize = math.ceil(mRows.toDouble / numByRow.toDouble).toInt
    val mBlockColSize = math.ceil(mColumns.toDouble / numByCol.toDouble).toInt
    val blksByRow = math.ceil(mRows.toDouble / mBlockRowSize).toInt
    val blksByCol = math.ceil(mColumns.toDouble / mBlockColSize).toInt
    val result = rows.mapPartitions(iter => {
      iter.flatMap( t => {
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
      })})
      .groupByKey()
      .mapPartitions( a => { a.map(
      input => {
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
            Logger.getLogger(getClass).
              log(Level.ERROR,"vectors:  " + input._2 +"Block Column Size dismatched" )
            throw new IOException("Block Column Size dismatched")
          }

          val rowOffset = vec.index.toInt - rowBase
          if (rowOffset >= smRows || rowOffset < 0) {
            Logger.getLogger(getClass).log(Level.ERROR,"Block Row Size dismatched" )
            throw new IOException("Block Row Size dismatched")
          }

          val tmp = vec.vector.toArray
          for (i <- 0 until tmp.length) {
            array(i * smRows + rowOffset) = tmp(i)
          }
        }

        val subMatrix = new BDM(smRows, smCols, array)
        (input._1, subMatrix)
      })}, true)

    new BlockMatrix(result, numRows(), numCols(), blksByRow, blksByCol)
  }

  /**
   * transform the DenseVecMatrix to SparseVecMatrix
   */
  def toSparseVecMatrix(): SparseVecMatrix = {
    val result = rows.mapPartitions( iter => {
      iter.map( t => {
        val array = t.vector.toArray
        val indices = new ArrayBuffer[Int]()
        val values = new ArrayBuffer[Double]()
        for (i <- 0 until array.length){
          if (array(i) != 0){
            indices += i
            values += array(i)
          }
        }
        if (indices.size > 0) {
          IndexSparseRow(t.index, new SparseVector(indices.size, indices.toArray, values.toArray))
        }else {}
      })
    })
    new SparseVecMatrix(result.asInstanceOf[RDD[IndexSparseRow]], numRows(), numCols())

  }
}
