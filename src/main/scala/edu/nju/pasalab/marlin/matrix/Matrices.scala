package edu.nju.pasalab.marlin.matrix

/**
 * Notice: some code is copy from mllib to make it compatible, and we change some of them
 */

import breeze.linalg.{Matrix => BM, DenseMatrix => BDM}

/**
 * Trait for a local matrix.
 */
trait Matrix extends Serializable {

  /** Number of rows. */
  def numRows: Int

  /** Number of columns. */
  def numCols: Int

  /** Converts to a dense array in column major. */
  def toArray: Array[Double]

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

  override def toArray: Array[Double] = values

  private[matrix] override def toBreeze: BM[Double] = new BDM[Double](numRows, numCols, values)
}

/**
 * Factory methods for [[org.apache.spark.mllib.linalg.Matrix]].
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
