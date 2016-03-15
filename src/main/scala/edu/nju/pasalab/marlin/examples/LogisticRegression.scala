package edu.nju.pasalab.marlin.examples

import edu.nju.pasalab.marlin.matrix.{BlockMatrix, DistributedVector, BlockID}
import edu.nju.pasalab.marlin.utils.{OnesGenerator, MTUtils}

import breeze.linalg.{DenseVector => BDV}
import org.apache.spark.{Partitioner, SparkContext, SparkConf}


object LogisticRegression {
  def main(args: Array[String]) {
    if (args.length < 1){
      println("usage: LogisticRegression <iterations> <stepSize>")
      System.exit(-1)
    }
    val conf = new SparkConf()//.setAppName("LR").setMaster("local[4]")
    conf.set("spark.default.parallelism", "4")
    val sc = new SparkContext(conf)
    val cores = 1

    val matVecPartitioner = new Partitioner {override def numPartitions: Int = 8 * cores
      override def getPartition(key: Any): Int = key match {
        case blkId: BlockID =>
          blkId.row
        case split: Int =>
          split
      }
    }

    val iterations = args(0).toInt
    val stepSize = args(1).toInt
    val instances = args(2).toInt
    val features = args(3).toInt
    val input = MTUtils.randomDenVecMatrix(sc, instances, features)
    val splits = 8 * cores
    val dataBlocks = input.toBlockMatrix(splits, 1).blocks.partitionBy(matVecPartitioner).cache()
    val data = new BlockMatrix(dataBlocks)
//    data.blocks.partitionBy(matVecPartitioner).cache()
    val dataTranspose = input.toBlockMatrix(1, 8).transpose()
    dataTranspose.blocks.cache()
    println(s" all the data are generated!")
    val labelVector = MTUtils.onesDistVector(sc, instances, splits).vectors.partitionBy(matVecPartitioner).cache()
    val labels = new DistributedVector(labelVector, instances, splits)
//    labels.vectors.partitionBy(matVecPartitioner).cache()

    var theta = BDV.ones[Double](features)
    for (i <- 1 to iterations) {
      println(s"in iteration $i")
      val q = data.multiply(theta).vectors.mapPartitions(vectors =>
        vectors.map(v => (v._1, v._2.inner.get.map(u => 1.0 / (1.0 + math.exp(-u))))))
      val h = new DistributedVector(q)
//      println(s"q first: ${q.first()._2(1 to 10).toArray.mkString(", ")}")
//      val t = h.substract(labels).vectors.collect()
      val t = labels.substract(h).vectors.collect()
      val tt = BDV.ones[Double](instances)
      var splitLen = math.ceil(instances.toDouble / t.length.toDouble).toInt
      for( (id, v) <- t){
        for ( j <- 0 until v.length) {
          tt.update(id * splitLen + j, v(j))
        }
      }
      val tmp = dataTranspose.multiply(tt).vectors.collect()
      println(s"tmp content 1 ${tmp(1)._2.toArray.mkString(", ")}")
      splitLen = math.ceil(features.toDouble / tmp.length.toDouble).toInt
      val grad = BDV.ones[Double](features)
      for( (id, v) <- tmp){
        for ( j <- 0 until v.length) {
          grad.update(id * splitLen + j, v(j) / (stepSize * instances * math.sqrt(i)))
        }
      }
//      println(s" grad content: ${grad(1 to 10).toArray.mkString(", ")}")
      theta = theta - grad
    }
    sc.stop()
    println(s"theta content: ${theta.toArray.mkString(", ")}")

  }
}
