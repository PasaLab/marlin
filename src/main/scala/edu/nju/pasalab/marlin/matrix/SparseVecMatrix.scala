package edu.nju.pasalab.marlin.matrix

import scala.collection.mutable.ArrayBuffer

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

class SparseVecMatrix(
    val rows: RDD[IndexSparseRow],
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
      val len = t.vector.indices.size
      for (i <- 0 until len){
        arr += ((t.vector.indices(i), (t.index, t.vector.values(i))))
      }
      arr
    }).groupByKey(partitioner).cache()

    val otherEmits = other.rows.flatMap(t => {
      val arr = new ArrayBuffer[(Long, (Long, Double))]()
      val len = t.vector.indices.size
      for ( i <- 0 until len){
        arr += ((t.index, (t.vector.indices(i), t.vector.values(i))))
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
}
