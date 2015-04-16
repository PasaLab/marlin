package edu.nju.pasalab.marlin.matrix

import scala.{specialized=>spec}
import breeze.linalg.{DenseVector => BDV}

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging


class DistributedVector(
  private [marlin] val vectors: RDD[(Int, BDV[Double])],
  private var len: Long) {

  def this(vectors: RDD[(Int, BDV[Double])]) = this(vectors, 0L)

  def splitNums = vectors.count()

  def length: Long = {
    if (len <= 0L) {
      len = vectors.aggregate(0)((a, b) => a + b._2.length, _ + _).toLong
    }
    len
  }

  def substract(v: DistributedVector): DistributedVector = {
    require(length == v.length, s"unsupported vector length: ${length} v.s ${v.length}")
    val result = vectors.join(v.vectors).map(t => (t._1, t._2._1 - t._2._2))
    new DistributedVector(result, v.length)
  }

  def getVectors = vectors
}
