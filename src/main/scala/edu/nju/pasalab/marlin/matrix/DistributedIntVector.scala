package edu.nju.pasalab.marlin.matrix

import scala.collection.mutable.ArrayBuffer
import scala.{specialized=>spec}
import breeze.linalg.{DenseVector => BDV, DenseMatrix => BDM, Transpose}

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkContext, Logging}

/**
 * a distributed view of long large vector, currently
 * it dose not support vector of which the length is larger than Int.MaxValue (2,147,483,647)
 * @param vectors
 * @param len the overall length of the vector
 */
class DistributedIntVector(private [marlin] val vectors: RDD[(Int, BDV[Int])],
                           private var len: Long, private var splits: Int) extends Serializable{

  private var columnMajor = true

  def this(vectors: RDD[(Int, BDV[Int])]) = this(vectors, 0L, 0)


  def isColumnMajor = columnMajor

  def setColumnMajor(boolean: Boolean) {
    columnMajor = boolean
  }

  def splitNum: Int = {
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

  def substract(v: DistributedIntVector): DistributedIntVector = {
    require(length == v.length, s"unsupported vector length: ${length} v.s ${v.length}")
    val result = vectors.join(v.vectors).map{case(id, (v1, v2)) => (id, v1 - v2)}
    new DistributedIntVector(result, v.length, splitNum)
  }

  def getVectors = vectors

  /**
   * a transposed view of the vector
   */
  def transpose(): DistributedIntVector = {
    val result = new DistributedIntVector(vectors, length, splitNum)
    result.setColumnMajor(false)
    result
  }

  /**
   * collect all the vectors to local breeze vector
   */
  def toBreeze() : BDV[Int] = {
    val vecs = vectors.collect()
    val result = BDV.zeros[Int](length.toInt)
    val offset = length.toInt / vecs.length
    for((id, v) <- vecs){
      result.slice(id * offset, id * offset + v.length) := v
    }
    result
  }

  /**
   * transform from distributed vector to another distributed vector
   * according to the split status
   * @param splitStatusByRow ATTENTION!!! the splitStatusByRow here means the splitstatus
   *                         of each partition, not each element in RDD
   * @param splitNum
   * @return
   */
  def toDisVector(splitStatusByRow: Array[ArrayBuffer[(Int, (Int, Int), (Int, Int))]],
                  splitNum: Int): DistributedIntVector = {
    val mostSplitLen = math.ceil(length.toDouble / splitNum.toDouble).toInt
    //    val splitLen = math.ceil(length.toDouble / mostSplitLen).toInt
    val vecs = vectors.mapPartitionsWithIndex{ (id, iter) =>
      val array = Array.ofDim[(Int, (Int, Int, BDV[Int]))](splitStatusByRow(id).size)
      var count = 0
      val vector = iter.next()._2
      for ((vecId, (oldStart, oldEnd), (newStart, newEnd)) <- splitStatusByRow(id)) {
        array(count) = (vecId, (newStart, newEnd, vector(oldStart to oldEnd)))
        count += 1
      }
      array.toIterator
    }.groupByKey().mapPartitions{ iter =>
      iter.map{ case(vecId, iterable) =>
        val vecLen = if ((vecId + 1) * mostSplitLen > length) {
          (length - vecId * mostSplitLen).toInt
        } else mostSplitLen
        val vector = BDV.zeros[Int](vecLen)
        val iterator = iterable.iterator
        for ((rowStart, rowEnd, vec) <- iterator) {
          vector(rowStart to rowEnd) := vec
        }
        (vecId, vector)
      }
    }
    //    val blocks = rows.mapPartitionsWithIndex { (id, iter) =>
    //      val array = Array.ofDim[(BlockID, (Int, Int, BDM[Double]))](splitStatusByRow(id).size)
    //      var count = 0
    //      for ((rowId, (oldRow1, oldRow2), (newRow1, newRow2)) <- splitStatusByRow(id)) {
    //        val rowBlock = oldRow2 - oldRow1 + 1
    //        val blk = BDM.zeros[Double](rowBlock, numCols().toInt)
    //        for (i <- 0 until rowBlock) {
    //          blk(i, ::) := iter.next()._2.t
    //        }
    //        array(count) = (BlockID(rowId, 1), (newRow1, newRow2, blk))
    //        count += 1
    //      }
    //      array.toIterator
    //    }.groupByKey().mapPartitions { iter =>
    //      iter.map { case (blkId, iterable) =>
    //        val rowLen = if ((blkId.row + 1) * mostSplitLen > numRows()) {
    //          (numRows() - blkId.row * mostSplitLen).toInt
    //        } else mostSplitLen
    //        val mat = BDM.zeros[Double](rowLen, numCols().toInt)
    //        val iterator = iterable.iterator
    //        for ((rowStart, rowEnd, blk) <- iterator) {
    //          mat(rowStart to rowEnd, ::) := blk
    //        }
    //        (blkId, mat)
    //      }
    //    }
    //    new BlockMatrix(blocks, numRows(), numCols(), splitLen, 1)
    new DistributedIntVector(vecs)
  }

//  /**
//   * multiply two distributed vector, if the left vector is a column-vector, and the right vector is a row-vector, then
//   * get a BlockMatrix result; if the left vector is row-vector and the right vector is a column-vector, then get
//   * a double variable.
//   * @param other
//   * @param mode "dist" mode means multiply two vector in distributed environment, while the "local" mode means get the
//   *             two vector local and then run computation
//   */
//  def multiply(other: DistributedVector, mode: String = "dist"): Either[Double, BlockMatrix] = {
//    require(length == other.length, s"the length of these two vectors are not the same")
//    require(splitNum == other.splitNum, s"currently, only support two vectors with the same splits")
//    if (columnMajor == true && other.columnMajor == false){
//      if (splitNum == other.splitNum){
//        val thisVecEmits = vectors.flatMap{
//          case(id, v) => Iterator.tabulate(splitNum)(i => (new BlockID(id, i), v))}
//        val otherVecEmits = other.getVectors.flatMap{
//          case(id, v) => Iterator.tabulate(splitNum)(i => (new BlockID(i, id), v))}
//        val blocks = thisVecEmits.join(otherVecEmits).map{
//          case(blkId, (v1, v2)) => (blkId, v1 * v2.t)}
//        val result = new BlockMatrix(blocks, length, length, splitNum, splitNum)
//        Right(result)
//      }else {
//        throw new IllegalArgumentException("currently, not supported for vectors with different dimension")
//      }
//    }else if (columnMajor == false && other.columnMajor == true){
//      val result: Double = if (mode.toLowerCase().equals("dist")) {
//        vectors.join(other.getVectors).map { case (id, (v1, v2)) =>
//          v1.t * v2
//        }.reduce(_ + _)
//      }else if (mode.toLowerCase().equals("local")){
//        val v1 = toBreeze().t
//        val v2 = other.toBreeze()
//        v1 * v2
//      }else {
//        throw new IllegalArgumentException("unrecognized mode")
//      }
//      Left(result)
//    }else {
//      throw new IllegalArgumentException("the columnMajor status of the two distributed vectors are the same")
//    }
//  }

}

object DistributedIntVector {
  def fromVector(sc: SparkContext, vector: BDV[Int], numSplits: Int): DistributedIntVector = {
    val vecLen = math.ceil(vector.length.toDouble / numSplits.toDouble).toInt
    val vectors = Iterator.tabulate[(Int, BDV[Int])](numSplits)(i => (i,
      vector.slice(i* vecLen, math.min((i + 1) * vecLen , vector.length)))).toSeq
    new DistributedIntVector(sc.parallelize(vectors, numSplits), vector.length, numSplits)
  }
}
