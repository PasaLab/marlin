package edu.nju.pasalab.marlin.examples

import org.apache.spark.{SparkContext, SparkConf}

/**
 * Created by v-yunta on 4/16/2015.
 */
object CountVectorLength {
  def main(args: Array[String]) {
    if (args.length < 2) {
      System.err.println("Usage: CountVectorLength <input> <output>")
      System.exit(1)
    }
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val data = sc.textFile(args(0))
    val lenCount = data.map { line =>
        val items = line.split(' ')
        (items.tail.length, 1)
      }.reduceByKey( _ + _).map(t => t._1 + "," + t._2)

    lenCount.coalesce(10).saveAsTextFile(args(1))
    sc.stop()
  }
}
