package edu.nju.pasalab.marlin.examples

import java.util.{Random, Calendar}

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.mllib.linalg.{Matrix, Vectors, Matrices, DenseMatrix}
import org.apache.spark.mllib.linalg.distributed.{IndexedRowMatrix, IndexedRow, BlockMatrix}
import org.apache.spark.serializer.KryoRegistrator
import org.apache.spark.{SparkContext, SparkConf}

import breeze.linalg.{DenseMatrix => BDM, DenseVector, Vector}


object MLlibMM {

  def toLocal(indMat: IndexedRowMatrix): Matrix = {
    val m = indMat.numRows().toInt
    val n = indMat.numCols().toInt
    val mat = BDM.zeros[Double](m, n)
    indMat.rows.collect().foreach { case IndexedRow(rowIndex, vector) =>
      val i = rowIndex.toInt
      mat(i, ::) := DenseVector(vector.toArray).t
    }
    new DenseMatrix(mat.rows, mat.cols, mat.data, mat.isTranspose)
  }


  def main(args: Array[String]) {
    if (args.length < 6) {
      println("usage: MLlibMM <matrixA row length> <matrixA column length> <matrixB column length> <mode> <m> <k> <n> ")
      println("mode 1 means BlockMatrix MM")
      println("mode 2 means IndexedRowMatrix MM")
      println("for example: MLlibMM 10000 10000 10000 1 6 6 6 ")
      System.exit(1)
    }
    val conf = new SparkConf()
    conf.set("spark.kryo.registrator", "edu.nju.pasalab.marlin.examples.MLlibRegistrator")
    val sc = new SparkContext(conf)
    val rowA = args(0).toInt
    val colA, rowB = args(1).toInt
    val colB = args(2).toInt
    val mode = args(3).toInt
    println("=========================================")
    mode match {
      case 1 =>
        val m = args(4).toInt
        val k = args(5).toInt
        val n = args(6).toInt
        println(s"MLlib RMM matrixA: $rowA by $colA ; matrixB: $rowB by $colB, " +
          s"m, k, n: $m, $k, $n; ${Calendar.getInstance().getTime}")
        val indicesA =  Array.ofDim[(Int, Int)](m*k)
        val indicesB =  Array.ofDim[(Int, Int)](k*n)
        for (i <- (0 until m)) {
          for (j <- (0 until k)){
            val ind = i * k + j
            indicesA(ind) = (i, j)
          }
        }
        for (i <- (0 until k)) {
          for (j <- (0 until n)){
            val ind = i * n + j
            indicesB(ind) = (i, j)
          }
        }
        val rowsPerBlockA = rowA / m
        val colsPerBlockA = colA / k
        val rng = new Random()
        val blocksA = sc.parallelize(indicesA, m*k).map{index =>
          (index, Matrices.rand(rowsPerBlockA, colsPerBlockA, rng))}
        val matA = new BlockMatrix(blocksA, rowsPerBlockA, colsPerBlockA, rowA, colA)

        val rowsPerBlockB = rowB / k
        val colsPerBlockB = colB / n
        val blocksB = sc.parallelize(indicesB, k*n).map{index =>
          (index, Matrices.rand(rowsPerBlockB, colsPerBlockB, rng))}
        val matB = new BlockMatrix(blocksB, rowsPerBlockB, colsPerBlockB, rowB, colB)
        val t0 = System.currentTimeMillis()
        val result = matA.multiply(matA)
        println(result.blocks.count())
        println(s"MLlib RMM used time ${(System.currentTimeMillis() - t0)} millis " +
          s";${Calendar.getInstance().getTime}")
      case 2 =>
        val rowsA = sc.parallelize(0L until rowA, 500).map(row =>
          IndexedRow(row, Vectors.dense(Vector.rand[Double](colA).toArray)))
        val rowsB = sc.parallelize(0L until rowB).map(row =>
          IndexedRow(row, Vectors.dense(Vector.rand[Double](colB).toArray)))
        val matA = new IndexedRowMatrix(rowsA, rowA, colA)
        val matB = new IndexedRowMatrix(rowsB, rowB, colB)
        val t0 = System.currentTimeMillis()
        val result = matA.multiply(toLocal(matB))
        result.rows.count()
        println(s"MLlib MapMM used time ${(System.currentTimeMillis() - t0)} millis " +
          s";${Calendar.getInstance().getTime}")
    }
    println("=========================================")
    sc.stop()
  }
}

class MLlibRegistrator extends KryoRegistrator{
  override def registerClasses(kryo: Kryo){
    kryo.register(classOf[BlockMatrix])
    kryo.register(classOf[org.apache.spark.mllib.linalg.DenseMatrix])
    kryo.register(classOf[org.apache.spark.mllib.linalg.Matrix])
    kryo.register(classOf[org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix])
  }
}
