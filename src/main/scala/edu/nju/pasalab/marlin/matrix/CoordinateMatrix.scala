package edu.nju.pasalab.marlin.matrix

import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.annotation.Experimental
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

/**
 * :: Experimental ::
 * Represents an entry in an distributed matrix.
 * @param codinate row index, column index
 * @param value value of the entry
 */
@Experimental
case class MatrixEntry(codinate: (Long, Long), value: Double)
/**
 * :: Experimental ::
 * Represents a matrix in coordinate format.
 *
 * @param entries matrix entries
 * @param nRows number of rows. A non-positive value means unknown, and then the number of rows will
 * be determined by the max row index plus one.
 * @param nCols number of columns. A non-positive value means unknown, and then the number of
 * columns will be determined by the max column index plus one.
 */
@Experimental
class CoordinateMatrix(
                        val entries: RDD[((Long, Long), Double)],
                        private var nRows: Long,
                        private var nCols: Long)  {
  /** Alternative constructor leaving matrix dimensions to be determined automatically. */
  def this(entries: RDD[((Long, Long), Double)]) = this(entries, 0L, 0L)
  /** Gets or computes the number of columns. */
  def numCols(): Long = {
    if (nCols <= 0L) {
      computeSize()
    }
    nCols
  }

  /** Gets or computes the number of rows. */
  def numRows(): Long = {
    if (nRows <= 0L) {
      computeSize()
    }
    nRows
  }

  /** Converts to IndexedRowMatrix. The number of columns must be within the integer range. */
  def toDenseVecMatrix(): DenseVecMatrix = {
    val nl = numCols()
    if (nl > Int.MaxValue) {
      sys.error(s"Cannot convert to a row-oriented format because the number of columns $nl is " +
        "too large.")
    }
    val n = nl.toInt
    val indexedRows = entries.map(entry => (entry._1._1, (entry._1._2.toInt, entry._2)))
      .groupByKey()
      .map { case (i, vectorEntries) =>
      (i, Vectors.dense(Vectors.sparse(n, vectorEntries.toSeq).toArray))
    }
    new DenseVecMatrix(indexedRows, numRows(), n)
  }

  /** Determines the size by computing the max row/column index. */
  private def computeSize() {
    // Reduce will throw an exception if `entries` is empty.
    val (m1, n1) = entries.map(entry => (entry._1._1, entry._1._2)).reduce { case ((i1, j1), (i2, j2)) =>
      (math.max(i1, i2), math.max(j1, j2))
    }
    // There may be empty columns at the very right and empty rows at the very bottom.
    nRows = math.max(nRows, m1 + 1L)
    nCols = math.max(nCols, n1 + 1L)
  }
  /** Collects data and assembles a local matrix. */
  private[marlin]  def toBreeze(): BDM[Double] = {
    val m = numRows().toInt
    val n = numCols().toInt
    val mat = BDM.zeros[Double](m, n)
    entries.collect().foreach { case ((i, j), value) =>
      mat(i.toInt, j.toInt) = value
    }
    mat
  }
}
