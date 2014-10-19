package edu.nju.pasalab.sparkmatrix


/**
 * This class overrides from [[org.apache.spark.mllib.linalg.distributed.IndexedRow]]
 * Notice: some code in this file is copy from MLlib to make it compatible
 */
case class IndexRow( index: Long, vector: Vector) {

  /**
   * Override 'toString' method to make the content in HDFS compatible
   */
  override def toString: String = {
    val result : StringBuilder = new StringBuilder(index.toString + ":")
    result.append(vector.toArray.mkString(","))

    result.toString()
  }

}

/**
 * This class overrides from [[org.apache.spark.mllib.linalg.distributed.IndexedRow]]
 * Notice: some code in this file is copy from MLlib to make it compatible
 */
case class IndexSparseRow( index: Long, vector: SparseVector) {

  /**
   * Override 'toString' method to make the content in HDFS compatible
   */
  override def toString: String = {
    val result : StringBuilder = new StringBuilder(index.toString + ":")
    result.append(vector.toArray.mkString(","))

    result.toString()
  }

}