package edu.nju.pasalab.sparkmatrix

import breeze.linalg.{DenseMatrix => BDM}

class Block(val blockID: BlockID, val matrix: BDM[Double]) extends Serializable{

  /** get the index row from the BlockID **/
  def blkIndexRow(): Int = {
    blockID.row
  }

  /** get the index column from the BlockID **/
  def blkIndexColumn(): Int = {
    blockID.column
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
        val other = that.asInstanceOf[BlockID]
        row == other.row && column == other.column && seq == other.seq
      case _ => false
    }

  override def hashCode(): Int = {
    row * 31  + column + seq
  }
}