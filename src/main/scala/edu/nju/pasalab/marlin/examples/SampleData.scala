package edu.nju.pasalab.marlin.examples

import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable

/**
 * Created by v-yunta on 4/16/2015.
 */
object SampleData {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: SampleData <input>  <output>")
      System.exit(1)
    }
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    val data1 = sc.parallelize(Seq((1, 1,2,3,4), (1, 4,2,5,4), (-1, 4,2,5,4), (-1, 0,2,3,0)))
//    data1.agg
//    val lines = scala.io.Source.fromFile(args(2)).getLines()
//    val map = new mutable.HashMap[String, String]
//    for( l <- lines){
//      val items = l.split(",")
//      map.+=((items(0), items(1)))
//    }
//    val bMap = sc.broadcast(map)
    val data = sc.textFile(args(0))
//    data.coalesce(160)
    val transformed = data.filter{ line =>
      line.split(' ').length >= 139}.map{line =>
      val items = line.split(' ')
        val label = items.head
        val array = items.tail.map { item =>
          //       if (bMap.value.contains(item.split(":")(0))){
          //         bMap.value.get(item.split(":")(0))
          //       }
          item.split(":")(0)
        }
        label + " " + array.mkString(" ")
    }.filter(line => !line.isEmpty)
    transformed.coalesce(80).saveAsTextFile(args(1))
    sc.stop()
  }

}
