package edu.nju.pasalab.marlin.matrix

import breeze.linalg.{DenseMatrix => BDM}


/**
 * Represents a distributively stored matrix backed by one or more RDDs.
 */
trait DistributedMatrix extends Serializable {

  /** Gets or computes the number of rows. */
  def numRows(): Long

  /** Gets or computes the number of columns. */
  def numCols(): Long

  /** Collects data and assembles a local dense breeze matrix (for test only). */
  private[matrix] def toBreeze(): BDM[Double]

  /** Element-wise add another element**/
  def add(d: Double): DistributedMatrix

  /** Matrix-matrix add another matrix**/
  def add(other: DistributedMatrix): DistributedMatrix

  /** Element-wise subtract another element**/
  def subtract(d: Double): DistributedMatrix

  /** Element-wise subtract by another element**/
  def subtractBy(d: Double): DistributedMatrix

  /** Matrix-matrix subtract another matrix**/
  def subtract(other: DistributedMatrix): DistributedMatrix

  /** Element-wise multiply another element**/
  def multiply(d: Double): DistributedMatrix

  /** Matrix-matrix multiply another matrix**/
  def multiply(other: DistributedMatrix, croes: Int): BlockMatrix

  /** Element-wise divide another element**/
  def divide(d: Double): DistributedMatrix

  /** Element-wise divide by another element**/
  def divideBy(d: Double): DistributedMatrix

  /** sum all the elements in matrix, note the Double.MaxValue = 1.7976931348623157E308 **/
  def sum(): Double

  /** Matrix-matrix dot product**/
  def dotProduct(other: DistributedMatrix): DistributedMatrix

  /** A transpose view of original matrix**/
  def transpose(): BlockMatrix

  /** Column bind to generate a new distributed matrix**/
  def cBind(other: DistributedMatrix): DistributedMatrix

  /** Save the matrix to filesystem in text format**/
  def saveToFileSystem(path: String)

  /** Save the matrix to filesystem in binary sequence format**/
  def saveSequenceFile(path: String)

  /** Print the matrix out, if the matrix is too large, it will print part**/
  def print()

  /** Print all the matrix out, if the matrix is too large, be careful to use this function**/
  def printAll()
}