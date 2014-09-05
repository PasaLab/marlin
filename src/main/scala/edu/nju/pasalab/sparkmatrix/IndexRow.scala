package edu.nju.pasalab.sparkmatrix



/**
 * Created by PASAlab@NJU on 14-7-29.
 */
case class IndexRow( index: Long, vector: Vector) {

  //def this(index: Long, vector: Vector) =  IndexedRow(index, vector)

  /**
   */
  override def toString: String = {
    val result : StringBuilder = new StringBuilder(index.toString + ":")
    result.append(vector.toArray.mkString(","))

    result.toString()
  }

}
