package edu.nju.pasalab.marlin.matrix

import scala.{specialized=>spec}
import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, Transpose}

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging

/**
 * a distributed view of long large vector, currently
 * it dose not support vector of which the length is larger than Int.MaxValue (2,147,483,647)
 * @param vectors
 * @param len the overall length of the vector
 */
class DistributedVector(
  private [marlin] val vectors: RDD[(Int, BDV[Double])],
  private var len: Long) {
  private var columnMajor = true
  def this(vectors: RDD[(Int, BDV[Double])]) = this(vectors, 0L)

  def isColumnMajor = columnMajor

  def splitNums = vectors.count().toInt

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

  /**
   * a transposed view of the vector
   */
  def transpose(): Unit = {
    vectors.map{case(index, vector) => (index, vector.t)}
    columnMajor = ! columnMajor
  }

  def multiply(vector: DistributedVector): Either[Double, BlockMatrix] = {
    require(length == vector.length, s"the length of these two vectors are not the same")
    if (columnMajor == true && vector.columnMajor == false){
      if (splitNums == vector.splitNums){
        val thisVecEmits = vectors.flatMap{
          case(id, v) => Iterator.tabulate(splitNums)(i => (new BlockID(id, i), v))}
        val otherVecEmits = vector.getVectors.flatMap{
          case(id, v) => Iterator.tabulate(splitNums)(i => (new BlockID(i, id), v))}
        val blocks = thisVecEmits.join(otherVecEmits).map{
          case(blkId, (v1, v2)) => (blkId, (v1 * v2.asInstanceOf[Transpose[BDV[Double]]]).asInstanceOf[BDM[Double]])}
        val result = new BlockMatrix(blocks, length, length, splitNums, splitNums)
        Right(result)
      }else {
        throw new IllegalArgumentException("currently, not supported for vectors with different dimension")
      }
    }else if (columnMajor == false && vector.columnMajor == true){
      val result = vectors.join(vector.getVectors).map{case(id, (v1, v2)) =>
        v1.asInstanceOf[Transpose[BDV[Double]]] * v2}.reduce(_ + _)
      Left(result)
    }else {
      throw new IllegalArgumentException("the columnMajor status of the two distributed vectors are the same")
    }
  }


}
