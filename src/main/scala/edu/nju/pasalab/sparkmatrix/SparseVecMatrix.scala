package edu.nju.pasalab.sparkmatrix

import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

import scala.collection.mutable.ArrayBuffer


class SparseVecMatrix(
    val sparseVecs: RDD[IndexSparseRow],
    private val numRows: Long,
    private val numCols: Long) {


  final def multiplySparse(other: SparseVecMatrix, parallelism: Int): CoordinateMatrix = {
    val partitioner = new HashPartitioner(parallelism)
    val thisEmits = sparseVecs.flatMap( t => {
      val arr = new ArrayBuffer[(Long, (Long, Double))]
      val len = t.vector.indices.size
      for (i <- 0 until len){
        arr += ((t.vector.indices(i), (t.index, t.vector.values(i))))
      }
      arr
    }).groupByKey(partitioner).cache()

    val otherEmits = other.sparseVecs.flatMap(t => {
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
    new CoordinateMatrix(result, numRows, other.numCols)
  }

}
