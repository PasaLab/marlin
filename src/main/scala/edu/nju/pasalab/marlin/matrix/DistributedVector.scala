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
  private var len: Long,
  private var splits: Int) extends Serializable{

  private var columnMajor = true

  def this(vectors: RDD[(Int, BDV[Double])]) = this(vectors, 0L, 0)


  def isColumnMajor = columnMajor

  def setColumnMajor(boolean: Boolean) {
    columnMajor = boolean
  }

  def splitNums: Int = {
    if (splits <= 0) {
      splits = vectors.count().toInt
    }
    splits
  }

  def length: Long = {
    if (len <= 0L) {
      len = vectors.aggregate(0)((a, b) => a + b._2.length, _ + _).toLong
    }
    len
  }

  def substract(v: DistributedVector): DistributedVector = {
    require(length == v.length, s"unsupported vector length: ${length} v.s ${v.length}")
    val result = vectors.join(v.vectors).map(t => (t._1, t._2._1 - t._2._2))
    new DistributedVector(result, v.length, splitNums)
  }

  def getVectors = vectors

  /**
   * a transposed view of the vector
   */
  def transpose(): DistributedVector = {
    val result = new DistributedVector(vectors, length, splitNums)
    result.setColumnMajor(false)
    result
  }

  /**
   * collect all the vectors to local breeze vector
  */
  def toBreeze() : BDV[Double] = {
    val vecs = vectors.collect()
    val result = BDV.zeros[Double](length.toInt)
    val offset = length.toInt / vecs.length
    for((id, v) <- vecs){
      result.slice(id * offset, id * offset + v.length) := v
    }
    result
  }

  /**
   * multiply two distributed vector, if the left vector is a column-vector, and the right vector is a row-vector, then 
   * get a BlockMatrix result; if the left vector is row-vector and the right vector is a column-vector, then get 
   * a double variable.
   * @param other
   * @param mode "auto" mode means multiply two vector in distributed environment, while the "local" mode means get the 
   *             two vector local and then run computation
   */
  def multiply(other: DistributedVector, mode: String = "auto"): Either[Double, BlockMatrix] = {
    require(length == other.length, s"the length of these two vectors are not the same")
    require(splitNums == other.splitNums, s"currently, only support two vectors with the same splits")
    if (columnMajor == true && other.columnMajor == false){
      if (splitNums == other.splitNums){
        val thisVecEmits = vectors.flatMap{
          case(id, v) => Iterator.tabulate(splitNums)(i => (new BlockID(id, i), v))}
        val otherVecEmits = other.getVectors.flatMap{
          case(id, v) => Iterator.tabulate(splitNums)(i => (new BlockID(i, id), v))}
        val blocks = thisVecEmits.join(otherVecEmits).map{
          case(blkId, (v1, v2)) => (blkId, v1 * v2.t)}
        val result = new BlockMatrix(blocks, length, length, splitNums, splitNums)
        Right(result)
      }else {
        throw new IllegalArgumentException("currently, not supported for vectors with different dimension")
      }
    }else if (columnMajor == false && other.columnMajor == true){
      val result: Double = if (mode.toLowerCase().equals("auto")) {
        vectors.join(other.getVectors).map { case (id, (v1, v2)) =>
          v1.t * v2
        }.reduce(_ + _)
      }else if (mode.toLowerCase().equals("local")){
        val v1 = toBreeze().t
        val v2 = other.toBreeze()
        v1 * v2
      }else {
        throw new IllegalArgumentException("unreconginzed mode")
      }
      Left(result)
    }else {
      throw new IllegalArgumentException("the columnMajor status of the two distributed vectors are the same")
    }
  }




}
