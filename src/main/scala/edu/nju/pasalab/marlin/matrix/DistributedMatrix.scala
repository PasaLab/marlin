package edu.nju.pasalab.marlin.matrix

import breeze.linalg.{DenseMatrix => BDM}

/**
 * Notice: the code in this file is copy from MLlib, to make it compatible
 */

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
}