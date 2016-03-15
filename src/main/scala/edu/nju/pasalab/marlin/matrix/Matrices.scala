package edu.nju.pasalab.marlin.matrix

/**
 * Notice: some code is copy from mllib to make it compatible, and we change some of them
 */

import breeze.linalg.{Matrix => BM, DenseMatrix => BDM, CSCMatrix}
import edu.nju.pasalab.marlin.utils.{UniformGenerator, RandomDataGenerator}

import scala.collection.mutable.{HashSet, ArrayBuffer}
import scala.util.Random

/**
 * Trait for a local matrix.
 */
trait Matrix extends Serializable {

  /** Number of rows. */
  def numRows: Int

  /** Number of columns. */
  def numCols: Int

  /** Converts to a breeze matrix. */
  private[matrix] def toBreeze: BM[Double]

  /** Gets the (i, j)-th element. */
  private[matrix] def apply(i: Int, j: Int): Double = toBreeze(i, j)

  override def toString: String = toBreeze.toString()
}

/**
 * Column-majored dense matrix.
 * The entry values are stored in a single array of doubles with columns listed in sequence.
 * For example, the following matrix
 * {{{
 *   1.0 2.0
 *   3.0 4.0
 *   5.0 6.0
 * }}}
 * is stored as `[1.0, 3.0, 5.0, 2.0, 4.0, 6.0]`.
 *
 * @param numRows number of rows
 * @param numCols number of columns
 * @param values matrix entries in column major
 */
class DenseMatrix(val numRows: Int, val numCols: Int, val values: Array[Double]) extends Matrix {

  require(values.length == numRows * numCols)

  def toArray: Array[Double] = values

  private[matrix] override def toBreeze: BM[Double] = new BDM[Double](numRows, numCols, values)
}

class SparseMatrix(val numRows: Int, val numCols: Int, val values: Array[SparseVector]) extends Matrix {
  private var nonZeros: Long = -1L

  private[marlin] def setNNZ(nnz: Long) {
    nonZeros = nnz
  }

  def this(rows: Int, cols: Int, values: Array[SparseVector], nnz: Long) = {
    this(rows, cols, values)
    setNNZ(nnz)
  }

  /** Converts to a breeze matrix in `CSCMatrix` format. */
  override private[marlin] def toBreeze: BM[Double] = {
    require(nonZeros < Int.MaxValue, s"Sparse Matrix contains ${nonZeros} elements, larger than Int.MaxValue")
    val colPtrs = Array.ofDim[Int](numCols + 1)
    var start = 0
    var c = 0
    val (data, rowIndices) = if (nonZeros > 0) {
      val d = Array.ofDim[Double](nonZeros.toInt)
      val r = Array.ofDim[Int](nonZeros.toInt)
      for (sv <- values){
        colPtrs(c) = start
        for(i <- 0 until sv.indices.get.size){
          r(i + start) = sv.indices.get(i)
          d(i + start) = sv.values.get(i)
        }
        start += sv.indices.get.size
        c += 1
      }
      (d, r)
    }else {
      val d = new ArrayBuffer[Double]()
      val r = new ArrayBuffer[Int]()
      for(sv <- values){
        colPtrs(c) = start
        d ++= sv.values.get
        r ++= sv.indices.get
        start += sv.indices.get.size
        c += 1
      }
      (d.toArray, r.toArray)
    }
    for(i <- c until numCols + 1){
      colPtrs(i) = data.size
    }
    new CSCMatrix[Double](data, numRows, numCols, colPtrs, rowIndices)
  }

  def toDense: BDM[Double] = {
    val c = Array.ofDim[Double](numRows * numCols)
    for(i <- 0 until numCols){
      if(values(i) != null && !values(i).isEmpty){
        val indices = values(i).indices.get
        val vals = values(i).values.get
        val offset = i * numRows
        for(k <- 0 until indices.size){
          c( offset + indices(k)) = vals(k)
        }
      }
    }
    BDM.create[Double](numRows, numCols, c)
  }


  private def vectMultiplyAdd(bval: Double, a: Array[Double], c: Array[Double],
                              aix: Array[Int], cix: Int, alen: Int) = {
    for(j <- 0 until alen){
      c(cix + aix(j)) +=  bval * a(j)
    }
  }

  def multiply(other: SparseMatrix): BDM[Double] = {
    val c = Array.ofDim[Double](numRows * other.numCols)
    var i , cix = 0
    while (i < other.numCols){
      val bcol = other.values(i)
      if(bcol != null && bcol.indices.get.size != 0) {
        val bix = bcol.indices.get
        val bvals = bcol.values.get
        for (k <- 0 until bix.size) {
          if (values(bix(k)) != null && values(bix(k)).indices.get.size != 0) {
            val bval = bvals(k)
            val acol = values(bix(k))
            val alen = acol.indices.get.size
            val aix = acol.indices.get
            val avals = acol.values.get
            vectMultiplyAdd(bval, avals, c, aix, cix, alen)
          }
        }
      }
      i += 1
      cix += numRows
    }
    new BDM[Double](numRows, other.numCols, c)
  }
}

object SparseMatrix {

  def rand(numRows: Int, numCols: Int, sparsity: Double): SparseMatrix = {
    val rnd = new Random()
    val generator = new UniformGenerator()
    val values = Array.ofDim[SparseVector](numCols)
    val sparseSize = (numCols * sparsity).toInt
    for(i <- 0 until numCols){
      val set = new HashSet[Int]()
      while (set.size < sparseSize) {
        set.+=(rnd.nextInt(numRows))
      }
      val indexes = set.toArray
      scala.util.Sorting.quickSort(indexes)
      values(i) = new SparseVector(numRows, indexes, Array.fill(sparseSize)(generator.nextValue()))
    }
    val nnz = sparseSize.toLong * numCols.toLong
    new SparseMatrix(numRows, numCols, values, nnz)
  }
}

/**
 * Factory methods
 */
object Matrices {

  /**
   * Creates a column-majored dense matrix.
   *
   * @param numRows number of rows
   * @param numCols number of columns
   * @param values matrix entries in column major
   */
  def dense(numRows: Int, numCols: Int, values: Array[Double]): Matrix = {
    new DenseMatrix(numRows, numCols, values)
  }

  /**
   * Creates a Matrix instance from a breeze matrix.
   * @param breeze a breeze matrix
   * @return a Matrix instance
   */
   def fromBreeze(breeze: BM[Double]): Matrix = {
    breeze match {
      case dm: BDM[Double] =>
        require(dm.majorStride == dm.rows,
          "Do not support stride size different from the number of rows.")
        new DenseMatrix(dm.rows, dm.cols, dm.data)
      case _ =>
        throw new UnsupportedOperationException(
          s"Do not support conversion from type ${breeze.getClass.getName}.")
    }
  }
}
