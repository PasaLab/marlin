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
    private var nCols: Int) extends DistributedMatrix{

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

  private [sparkmatrix]  def toBreeze(): BDM[Double] = {
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
   * A matrix multiply another IndexMatrix
   *
   * @param other another matrix in IndexMatrix format
   * @param blkNum is the split nums of submatries, if you set it as 10,
   *               which means you split every original large matrix into 10*10=100 blocks.
   *               The smaller this argument, the biger every worker get submatrix.
   *               When doing experiments, we multiply two 20000 by 20000 matrix together, we set it as 10.
   */
  final def multiply(other: IndexMatrix, blkNum: Int): IndexMatrix = {
    require(this.numCols == other.numRows, s"Dimension mismatch: ${this.numCols} vs ${other.numRows}")

    val mRows = this.numRows()
    val mCols = other.numCols()
    val mBlcokRowSize = math.ceil(mRows.toDouble / blkNum.toDouble).toInt
    val mBlcokColSize = math.ceil(mCols.toDouble / blkNum.toDouble).toInt
    //generate 'collection table' from these two matrices
    val mAcollects = collectTable(this, blkNum, true)
    val mBcollects = collectTable(other, blkNum, false)
    val result = mAcollects.join(mBcollects).
      map(t => {
      val t1 = System.currentTimeMillis()
      val b1 = t._2._1.toBreeze.asInstanceOf[BDM[Double]]
      val b2 = t._2._2.toBreeze.asInstanceOf[BDM[Double]]
      val t2 = System.currentTimeMillis()

      Logger.getLogger(this.getClass).log(Level.INFO, "to breeze time: " + (t2 - t1).toString + " ms")
      Logger.getLogger(this.getClass).log(Level.INFO, "b1 rows: " + b1.rows + " , b1 cols: " + b1.cols)
      Logger.getLogger(this.getClass).log(Level.INFO, "b2 rows: " + b2.rows + " , b2 cols: " + b2.cols)
      val result = b1 * b2
      val t3 = System.currentTimeMillis()
      Logger.getLogger(this.getClass).log(Level.INFO, "breeze multiply time: " + (t3 - t2).toString + " ms")
      val resultMat = Matrices.fromBreeze(result)
      val t4 = System.currentTimeMillis()
      Logger.getLogger(this.getClass).log(Level.INFO, "from breeze time: " + (t4 - t3).toString + " ms")

      (new BlockID(t._1.row, t._1.column), resultMat)
      })
      .groupByKey()
      .flatMap(t => {
      val list = t._2.toArray
      val smRows = list(0).numRows
      val smCols = list(0).numCols
      val res = Array.ofDim[Double](smRows * smCols)
      for (elem <- list) {
        val array = elem.toArray
        for (i <- 0 until array.length) {
          res(i) += array(i)
        }
      }
      var arrayBuf = new ArrayBuffer[(Int, Int, Double)]
      for (i <- 0 until res.length) {
        val b: Int = i / smRows
        arrayBuf += ((i - b * smRows + t._1.row * mBlcokRowSize, b + t._1.column * mBlcokColSize, res(i)))
      }
      arrayBuf
      })
      .map(t => (t._1, (t._2, t._3)))
      .groupByKey()
      .map(genIndexRow)

    new IndexMatrix(result)

  }

  /**
   * This function is still in progress !
   * LU decompose this IndexMatrix to generate a lower triangular matrix L and a upper triangular matrix U
   *
   * @return a pair (lower triangular matrix, upper triangular matrix)
   */
  def luDecompose(mode: String = "auto"): (IndexMatrix, IndexMatrix) = {
    val iterations = this.numRows
    require(iterations == this.numCols, s"currently we only support square matrix: ${iterations} vs ${this.numRows}")

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
    require(this.numRows == other.numRows, s"Dimension mismatch: ${this.numRows} vs ${other.numRows}")
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
    require(this.numRows == other.numRows, s"Dimension mismatch: ${this.numRows} vs ${other.numRows}")
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

  /**
   * function used in multiply
   */
  private [sparkmatrix] def collectTable(matrix: IndexMatrix, blkNum: Int, matrixPos: Boolean)
  :RDD[(BlockID, DenseMatrix)] = {
    //val log = Logger.getLogger(this.getClass)
    val mRows = matrix.numRows().toInt
    val mColumns = matrix.numCols().toInt
    val mBlockRowSize = math.ceil(mRows.toDouble / blkNum.toDouble).toInt
    val mBlockColSize = math.ceil(mColumns.toDouble / blkNum.toDouble).toInt
    //get subVector according to34 the column size and is keyed by the blockID
    matrix.rows.flatMap( t =>{
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
        arrayBuf})
      .groupByKey()
      //emit the block according to the new BlockID
      .flatMap(input => {
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

        val subMatrix = new DenseMatrix(smRows, smCols, array)
        var arrayBuf = new ArrayBuffer[(BlockID , DenseMatrix)]
        if (matrixPos) {
          for (x <- 0 to blkNum - 1) {
            val r: Int = input._1.row * blkNum * blkNum
            val seq: Int = x * blkNum + input._1.column+ r
            arrayBuf += ((new BlockID( input._1.row, x, seq), subMatrix))
          }
        }else{
          for (x <- 0 to blkNum - 1) {
            val seq = x * blkNum * blkNum + input._1.column * blkNum + input._1.row
            arrayBuf += ((new BlockID(x, input._1.column, seq), subMatrix))
          }
        }
        arrayBuf
    })
  }

  /**
   * function used in multiply
   */
  private [sparkmatrix] def genIndexRow(input: ( Int, Iterable[( Int, Double)] )): IndexRow = {
    val itr = input._2.toList
    val array = Array.ofDim[Double](itr.length)
    for (it <- itr) {
      array(it._1) += it._2
    }
    new IndexRow(input._1 , Vectors.dense(array))
  }

}
