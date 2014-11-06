package edu.nju.pasalab.marlin.matrix

import scala.collection.mutable.ArrayBuffer

import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import edu.nju.pasalab.marlin.utils.MTUtils

class SparseVecMatrix(
    val rows: RDD[(Long, SparseVector)],
    val nRows: Long,
    val nCols: Long) {

  /**
   * multiply another matrix in SparseVecMatrix type
   *
   * @param other another matrix to be multiplied in SparseVecMatrix type
   * @param parallelism the parallelism during the multiply process
   */
  final def multiplySparse(other: SparseVecMatrix, parallelism: Int): CoordinateMatrix = {
    val partitioner = new HashPartitioner(parallelism)
    val thisEmits = rows.flatMap( t => {
      val arr = new ArrayBuffer[(Long, (Long, Double))]
      val len = t._2.indices.size
      for (i <- 0 until len){
        arr += ((t._2.indices(i), (t._1, t._2.values(i))))
      }
      arr
    }).groupByKey(partitioner).cache()

    val otherEmits = other.rows.flatMap(t => {
      val arr = new ArrayBuffer[(Long, (Long, Double))]()
      val len = t._2.indices.size
      for ( i <- 0 until len){
        arr += ((t._1, (t._2.indices(i), t._2.values(i))))
      }
      arr
    })

    val result = thisEmits.join(otherEmits).flatMap( t =>{
      val arr = new ArrayBuffer[((Long, Long),Double)]()
      val list = t._2._1.toArray
      for (l <- list){
        arr += (((l._1, t._2._2._1), l._2 * t._2._2._2))
      }
      arr
    }).partitionBy(partitioner).persist().reduceByKey( _ + _)
    new CoordinateMatrix(result, nRows, other.nCols)
  }

 /** transform the DenseVecMatrix to BlockMatrix
  *
  * @return original matrix in DenseVecMatrix type
  */
  def toDenseVecMatrix(): DenseVecMatrix = {
    val mat = MTUtils.zerosDenVecMatrix(rows.context, nRows, nCols.toInt)
    val result = mat.rows.leftOuterJoin(rows).map(t => {
      if (t._2._2.isEmpty){
        (t._1, t._2._1)
      } else
      (t._1, Vectors.fromBreeze((t._2._1.toBreeze.+=(t._2._2.get.toBreeze)).asInstanceOf[BDV[Double]]))
    })
    new DenseVecMatrix(result, nRows, nCols)
  }
}
