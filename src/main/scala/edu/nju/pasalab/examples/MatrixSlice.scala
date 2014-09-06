package edu.nju.pasalab.examples

import org.apache.spark.{SparkContext, SparkConf}

import edu.nju.pasalab.sparkmatrix.{IndexMatrix, IndexRow, Vectors}

/**
 * Test get sub matrix by row or column
 * Only in spark-shell, you can see the print result.
 */
object MatrixSlice {

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
     val matrix = new IndexMatrix(rows)
     println("matrix contents (rows and columns index both start from 0):")
     matrix.rows.foreach( t => println(t.toString))

     println("\nget sub matrix slice by row from 1 to 3 ")
     matrix.sliceByRow(1,3).rows.foreach( t => println(t.toString) )

     println("\nget sub matrix slice by column from 1 to 4 ")
     matrix.sliceByColumn(1,4).rows.foreach( t => println(t.toString) )

     println("\nget sub matrix slice by row from 1 to 3, and slice by column from 1 to 3 ")
     matrix.getSubMatrix(1,3,1,3).rows.foreach( t => println(t.toString) )
    sc.stop()
  }
}
