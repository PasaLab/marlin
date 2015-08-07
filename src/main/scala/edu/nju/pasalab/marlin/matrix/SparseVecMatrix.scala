package edu.nju.pasalab.marlin.matrix

import scala.collection.mutable.ArrayBuffer

import breeze.linalg.{DenseVector => BDV, SparseVector => BSV}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import edu.nju.pasalab.marlin.utils.MTUtils

class SparseVecMatrix(
    val rows: RDD[(Long, BSV[Double])],
    val nRows: Long,
    val nCols: Long) {

  /**
   * multiply another matrix in SparseVecMatrix type
   *
   * @param other another matrix to be multiplied in SparseVecMatrix type
   */
  final def multiplySparse(other: SparseVecMatrix): CoordinateMatrix = {
    val thisEmits = rows.flatMap{case(index, values) =>
      val arr = new ArrayBuffer[(Long, (Long, Float))]
      val len = values.index.size
      for (i <- 0 until len){
        arr += ((values.index(i), (index, values.data(i).toFloat)))
      }
      arr
    }.groupByKey()

    val otherEmits = other.rows.flatMap{case(index, values) =>
      val arr = new ArrayBuffer[(Long, (Long, Float))]()
      val len = values.index.size
      for ( i <- 0 until len){
        arr += ((index, (values.index(i), values.data(i).toFloat)))
      }
      arr
    }


    val result = thisEmits.join(otherEmits).flatMap{case(index, (valA, valB)) =>
      val arr = new ArrayBuffer[((Long, Long), Float)]()
      for (l <- valA){
        arr += (((l._1, valB._1), l._2 * valB._2))
      }
      arr
    }.reduceByKey( _ + _)
    new CoordinateMatrix(result, nRows, other.nCols)
  }

 /** transform the DenseVecMatrix to BlockMatrix
  *
  * @return original matrix in DenseVecMatrix type
  */
  def toDenseVecMatrix(): DenseVecMatrix = {
    val mat = MTUtils.zerosDenVecMatrix(rows.context, nRows, nCols.toInt)
    val result = mat.rows.leftOuterJoin(rows).map{case(id, (denseVec, sparseVec)) => {
      if (sparseVec.isEmpty){
        (id, denseVec)
      } else
      (id, (denseVec +=(sparseVec.get)))
    }}
    new DenseVecMatrix(result, nRows, nCols)
  }

  def elementsCount(): Long = {
    rows.count()
  }

}
