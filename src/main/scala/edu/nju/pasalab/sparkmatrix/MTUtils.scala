package edu.nju.pasalab.sparkmatrix

import org.apache.spark.SparkContext

object MTUtils {

  /**
   * Function to design the method how to split input two matrices
   *
   * @param m rows num of Matrix A
   * @param k columns num of Matrix A, also rows num of Matrix B
   * @param n columns num of Matrix B
   * @param cores all the cores cross the cluster
   * @return rows of Matrix A to be split nums, columns of Matrix A to be split nums,
   *         columns of Matrix B to be split nums
   */

  def splitMethod(m: Long, k: Long, n: Long, cores: Int): (Int, Int, Int) = {
    var mSplitNum = 1
    var kSplitNum = 1
    var nSplitNum = 1
    var _m = m
    var _k = k
    var _n = n
    var _cores = cores
    while (_cores > 1) {
      if (dimToSplit(_m, _k, _n) == 1) {
        nSplitNum *= 2
        _n /= 2
        _cores /= 2
      } else if (dimToSplit(_m, _k, _n) == 2) {
        mSplitNum *= 2
        _m /= 2
        _cores /= 2
      } else {
        kSplitNum *= 2
        _k /= 2
        _cores /= 2
      }
    }
    (mSplitNum, kSplitNum, nSplitNum)
  }

  private  def dimToSplit(m: Long, k: Long, n: Long): Int={
    var result = 0
    if (n >= k && n >= m) {
      result = 1
    }else if (m >= k && m >= n) {
      result = 2
    }
    else result = 3
    result
  }

  /**
   * Function to load matrix from file
   *
   * @param sc the running SparkContext
   * @param path the path where store the matrix
   * @param minPartition the min num of partitions of the matrix to load in Spark
   */
  def loadMatrixFile(sc: SparkContext, path: String, minPartition: Int = 16): IndexMatrix = {
    if (!path.startsWith("hdfs://") && !path.startsWith("tachyon://") && !path.startsWith("file://")) {
      System.err.println("the path is not in local file System, HDFS or Tachyon")
      System.exit(1)
    }
    val file = sc.textFile(path, minPartition)
    val rows = file.map(t =>{
      val e = t.split(":")
      val rowIndex = e(0).toLong
      val vec = Vectors.dense(e(1).split(",").map(_.toDouble))
      IndexRow(rowIndex,vec)
    })
    new IndexMatrix(rows)
  }

  /**
   * Function to load matrix from a dictionary which contains many files to generate a Matrix
   *
   * @param sc the running SparkContext
   * @param path the path where store the matrix
   * @param minPartition the min num of partitions of the matrix to load in Spark
   */
  def loadMatrixFiles(sc: SparkContext, path: String, minPartition: Int = 16): IndexMatrix = {
    if (!path.startsWith("hdfs://") && !path.startsWith("tachyon://") && !path.startsWith("file://")) {
      System.err.println("the path is not in local file System, HDFS or Tachyon")
      System.exit(1)
    }
    val files = sc.wholeTextFiles(path, minPartition)
    val rows = files.flatMap(t =>{
      val lines = t._2.split("\n")
      lines.map( l =>{
        val content = l.split(":")
        IndexRow( content(0).toLong, Vectors.dense( content(1).split(",").map(_.toDouble) ))
      })
    })
    new IndexMatrix(rows)
  }

  /**
   * Function to generate a IndexMatrix from a Array[Array[Double]
   *
   * @param sc the running SparkContext
   * @param array the two dimension Double array
   * @param partitions the default num of partitions when you create an RDD, you can set it by yourself
   */
  def arrayToMatrix(sc:SparkContext , array: Array[Array[Double]] , partitions: Int = 2): IndexMatrix ={
    new IndexMatrix( sc.parallelize(array.zipWithIndex.
      map{ case(t,i)  => IndexRow(i, Vectors.dense(t)) },partitions) )
  }

  /**
   * Function to transform a IndexMatrix to a Array[Array[Double]], but now we use collect method to
   * get all the content from distributed matrices which means it is expensive, so currently only
   * not so large matrix can transform to Array[Array[Double]]
   *
   * @param mat the IndexMatrix to be transformed
   */

  def matrixToArray(mat: IndexMatrix ): Array[Array[Double]] ={
    val arr = Array.ofDim[Double](mat.numRows().toInt, mat.numCols().toInt)
    mat.rows.collect().foreach( t => t.vector.toArray.copyToArray(arr(t.index.toInt)) )
    arr
  }

}