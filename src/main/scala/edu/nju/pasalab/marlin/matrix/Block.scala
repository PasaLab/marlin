package edu.nju.pasalab.marlin.matrix

import breeze.linalg.{DenseMatrix => BDM}

case class Block(val blockID: BlockID, val matrix: BDM[Double]) extends Serializable{

  /** get the index row from the BlockID **/
  def blkIndexRow(): Int = {
    blockID.row
  }

  /** get the index column from the BlockID **/
  def blkIndexColumn(): Int = {
    blockID.column
  }

  /**
   * used when saveAsTextFile, format is
   * 'blockID.row'-'blockID.column'-'matrix.rows'-'matrix.cols':'matrix.array'
   * note that array in a matrix is column major
   */
  override def toString: String = {
    val result = new StringBuilder(blockID.row + "-" + blockID.column
      + "-" + matrix.rows + "-" + matrix.cols + ":")
    result.append(matrix.data.mkString(","))
    result.toString()
  }

}

/**
 * BlockID is the ID of Blocks split from the Matrix
 *
 * @param row starts form 0 to num_of_rows -1
 * @param column starts form 0 to num_of_columns-1
 */
class BlockID(val row: Int ,val column: Int, val seq: Int = 0) extends Serializable {

  override def equals(other: Any) :Boolean =
    other match {
      case that: BlockID =>
        row == that.row && column == that.column && seq == that.seq
      case _ => false
    }

  override def hashCode(): Int = {
    row * 31  + column + seq
  }
}
