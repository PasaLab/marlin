package edu.nju.pasalab.marlin.examples

import breeze.linalg.{DenseVector => BDV}

import edu.nju.pasalab.marlin.matrix.{DenseVector, DistributedVector, DenseVecMatrix, Vectors}
import edu.nju.pasalab.marlin.utils.MTUtils
import org.apache.spark.{SparkContext, SparkConf}

import scala.collection.mutable.ArrayBuffer


object PageRank {

  def loadLinksMatrix(sc: SparkContext, input: String, linkNum: Int): DenseVecMatrix = {
    val lines = sc.textFile(input, 2)
    val links = lines.map{s =>
      val parts = s.split("\\s+")
      (parts(0).toLong, parts(1).toInt)
    }.groupByKey().map{ case (page, links) =>
        val vector = new Array[Double](linkNum)
        for( l <- links){
          vector(l - 1) = 1.0 / links.size
        }
      (page - 1, BDV(vector))
    }
    new DenseVecMatrix(links, linkNum.toLong, linkNum.toLong)
  }

  def main(args: Array[String]) {
    if (args.length < 1) {
      System.err.println("Usage: PageRank <file> <iteration> <linkNum>")
      System.exit(1)
    }
    val conf = new SparkConf()
    val iteration = if (args.length > 1) args(1).toInt else 0
    val sc = new SparkContext(conf)
    val linkNum = if  (args.length > 2) args(2).toInt else 8
    val numBlocks = 2
    val linkRows = loadLinksMatrix(sc, args(0), linkNum)
    linkRows.printAll()
    val links = linkRows.transpose(numBlocks).multiply(0.85)
    links.blocks.cache()
    var ranks = MTUtils.onesDistVector(sc, linkNum, numBlocks)
    val r: DenseVector = new DenseVector((BDV.ones[Double](linkNum) * 0.15).asInstanceOf[BDV[Double]])
    ranks.getVectors.collect().foreach(println _)
    for( i <- 0 until iteration){
      println(s"in iteration $i")
      val result = links.multiply(ranks).getVectors.reduce((a, b) => (0, a._2.add(b._2)))._2.add(r)
      val vectorLength = result.length
      val splitLen = math.ceil(vectorLength.toDouble / numBlocks.toDouble).toInt
      val arrayBuffer = new ArrayBuffer[(Int, BDV[Double])]()
      for ( j <- 0 until numBlocks) {
        val start: Int = j * splitLen
        val end: Int =  math.min((j + 1) * splitLen - 1, vectorLength - 1)
        arrayBuffer.+=((j, result.inner.get(start to end)))
      }
       ranks = new DistributedVector(sc.parallelize(arrayBuffer, numBlocks))
    }
    ranks.vectors.collect().foreach(tup => println("ranks: " + tup._2.toArray.mkString(", ")))
  }
}
