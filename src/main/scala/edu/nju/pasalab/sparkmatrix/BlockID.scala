package edu.nju.pasalab.sparkmatrix

/**
 * Created by PASAlab@NJU on 14-7-11.
 */


/**
 *    BlockID is the ID of Blocks split from the Matrix
 *    row starts form 0 to num_of_rows -1
 *    column starts form 0 to num_of_columns-1
 */

class BlockID(val row: Int ,val column: Int, val seq: Int = 0) extends Serializable {

  /**
   *
   */
   override def equals(other: Any) :Boolean =
     other match {
       case that: BlockID =>
         val other = that.asInstanceOf[BlockID]
         row == other.row && column == other.column && seq == other.seq
       case _ => false
      }

  /**
   *
   */
   override def hashCode(): Int = {
     row * 31  + column + seq
   }
}