package edu.nju.pasalab.marlin.examples

import org.apache.spark.{SparkContext, SparkConf}

import edu.nju.pasalab.marlin.matrix.{DenseVecMatrix, IndexRow, Vectors}

/**
 * Test element-element wise operations
 * Only in spark-shell or local mode, you can see the print result.
 */
object MatrixElemOP {

  def main (args: Array[String]) {
    val conf = new SparkConf().setAppName("test IndexMatrix Slice Operations")
    val sc = new SparkContext(conf)
    val data = Seq(
      (0L, Vectors.dense(1.0, 2.0, 3.0, 4.0, 5.0, 6.0)),
      (1L, Vectors.dense(2.0, 2.0, 2.0, 2.0, 2.0, 2.0)),
      (2L, Vectors.dense(3.0, 3.0, 3.0, 3.0, 3.0, 3.0)),
      (3L, Vectors.dense(4.0, 4.0, 4.0, 4.0, 4.0, 4.0)),
      (4L, Vectors.dense(5.0, 5.0, 5.0, 5.0, 5.0, 5.0))
    ).map( x => IndexRow(x._1 , x._2))
    val rows = sc.parallelize(data,2)
    val matrixA = new DenseVecMatrix(rows)

    val dataB = Seq(
      (0L, Vectors.dense(6.0, 5.0, 4.0, 3.0, 2.0, 1.0)),
      (1L, Vectors.dense(6.0, 6.0, 6.0, 6.0, 6.0, 6.0)),
      (2L, Vectors.dense(5.0, 5.0, 5.0, 5.0, 5.0, 5.0)),
      (3L, Vectors.dense(4.0, 4.0, 4.0, 4.0, 4.0, 4.0)),
      (4L, Vectors.dense(3.0, 3.0, 3.0, 3.0, 3.0, 3.0))
    ).map( x => IndexRow(x._1 , x._2))

    val rowsB = sc.parallelize(dataB,2)
    val matrixB = new DenseVecMatrix(rowsB)

    println("\nmatrix A contents (rows and columns index both start from 0):")
    matrixA.rows.foreach( t => println(t.toString))

    println("\nmatrix A contents (rows and columns index both start from 0):")
    matrixB.rows.foreach( t => println(t.toString))

    println("\nmatrix A add matrix B:")
    matrixA.add(matrixB).rows.foreach(t => println(t.toString) )

    println("\nmatrix A minus matrix B:")
    matrixA.subtract(matrixB).rows.foreach(t => println(t.toString) )

    println("\nmatrix A element-wise add 2:")
    matrixA.add(2).rows.foreach(t => println(t.toString) )

    println("\nmatrix A element-wise minus 2:")
    matrixA.subtract(2).rows.foreach(t => println(t.toString) )

    println("\nmatrix A element-wise multiply 2:")
    matrixA.multiply(2).rows.foreach(t => println(t.toString) )

    println("\nmatrix A element-wise divide 2:")
    matrixA.divide(2).rows.foreach(t => println(t.toString) )
    sc.stop()
  }

}
