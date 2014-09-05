package edu.nju.pasalab.sparkmatrix

import org.apache.spark.SparkContext


/**
 * Created by PASAlab@NJU on 14-7-24.
 */
object MTUtils {

  /**
   * function to load matrix from file
   */
  def loadMatrixFile(sc: SparkContext, path: String, minPatition: Int = 3): IndexMatrix = {
    if (!path.startsWith("hdfs://") && !path.startsWith("tachyon://") && !path.startsWith("file://")) {
      System.err.println("the path is not in local file System, HDFS or Tachyon")
      System.exit(1)
    }
    val file = sc.textFile(path, minPatition)
    val rows = file.map(t =>{
      val e = t.split(":")
      val rowIndex = e(0).toLong
      val vec = Vectors.dense(e(1).split(",").map(_.toDouble))
      IndexRow(rowIndex,vec)
    })

    val res = new IndexMatrix(rows)
    res
  }

  /**
   * function to load matrix from a dictionary contains many files to generate a Matrix
   */
  def loadMatrixFiles(sc: SparkContext, path: String, minPatition: Int = 3): IndexMatrix = {
    if (!path.startsWith("hdfs://") && !path.startsWith("tachyon://") && !path.startsWith("file://")) {
      System.err.println("the path is not in local file System, HDFS or Tachyon")
      System.exit(1)
    }
    val file = sc.wholeTextFiles(path, minPatition)
    val rows = file.map(t =>{
      val e = t._2.split(":")
      val rowIndex = e(0).toLong
      val vec = Vectors.dense(e(1).split(",").map(_.toDouble))
      IndexRow(rowIndex,vec)
    })

    val res = new IndexMatrix(rows)
    res
  }

  def arrayToMatrix(sc:SparkContext , array: Array[Array[Double]] , para: Int = 2): IndexMatrix ={
    new IndexMatrix( sc.parallelize(array.zipWithIndex.
      map{ case(t,i)  => IndexRow(i, Vectors.dense(t)) },para) )
  }

  def matrixToArray(mat: IndexMatrix ): Array[Array[Double]] ={
    val arr = Array.ofDim[Double](mat.numRows().toInt, mat.numCols().toInt)
    mat.rows.collect().foreach( t => t.vector.toArray.copyToArray(arr(t.index.toInt)) )
    arr
  }

}