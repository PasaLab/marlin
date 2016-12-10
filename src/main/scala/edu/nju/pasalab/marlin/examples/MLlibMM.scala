package edu.nju.pasalab.marlin.examples

import java.util.{Random, Calendar}

import com.esotericsoftware.kryo.Kryo
import edu.nju.pasalab.marlin.utils.MTUtils
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
    if (args.length < 4) {
        println("usage: MLlibMM <matrixA row length> <matrixA column length> <matrixB column length> <mode> <m> <k> <n> ")
        println("mode 1 means BlockMatrix MM")
        println("mode 2 means IndexedRowMatrix MM (MapMM)")
        println("mode 3 means IndexedRowMatrix MM (RMM): " +
          "MLlibMM <matrixA row length> <matrixA column length> <matrixB column length> <mode> <m> <k> <n>")
        println("mode 4 means IndexedRowMatrix MM (RMM) with direct rowsPerBlock : " +
          " MLlibMM <matrixA row length> <matrixA column length> <matrixB column length> <mode> <rowsPerBlockA> <colsPerBlockA> <colsPerBlockB>")
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

        val rowsPerBlockA = math.ceil(1.0 * rowA / m).toInt
        val colsPerBlockA = math.ceil(1.0 * colA / k).toInt
        val rng = new Random()
        val blocksA = sc.parallelize(indicesA, m*k).map{index =>
          val r = if (rowA < rowsPerBlockA * (index._1 + 1)){
            rowA - rowsPerBlockA * index._1
          }else rowsPerBlockA
          val c = if (colA < colsPerBlockA * (index._2 + 1)){
            colA - colsPerBlockA * index._2
          }else colsPerBlockA
          (index, Matrices.rand(r, c, rng))}

        val matA = new BlockMatrix(blocksA, rowsPerBlockA, colsPerBlockA, rowA, colA)
        // this step used to distribute blocks uniformly across the custer
        matA.blocks.count()
        val rowsPerBlockB = math.ceil(1.0 * rowB / k).toInt
        val colsPerBlockB = math.ceil(1.0 * colB / n).toInt
        val blocksB = sc.parallelize(indicesB, k*n).map{index =>
          val r = if (rowA < rowsPerBlockB * (index._1 + 1)){
            rowA - rowsPerBlockB * index._1
          }else rowsPerBlockB
          val c = if (colA < colsPerBlockB * (index._2 + 1)){
            colA - colsPerBlockB * index._2
          }else colsPerBlockB
          (index, Matrices.rand(r, c, rng))}

        val matB = new BlockMatrix(blocksB, rowsPerBlockB, colsPerBlockB, rowB, colB)
        // this step used to distribute blocks uniformly across the custer
        matB.blocks.count()
        val t0 = System.currentTimeMillis()
        val result = matA.multiply(matB)
        println(result.blocks.count())
        println(s"MLlib RMM used time ${(System.currentTimeMillis() - t0)} millis " +
          s";${Calendar.getInstance().getTime}")
      case 2 =>
        val rowsA = sc.parallelize(0L until rowA, 640).map(row =>
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
      case 3 =>
        val m = args(4).toInt
        val k = args(5).toInt
        val n = args(6).toInt
        val parallelism = 320
        println(s"MLlib RMM fom IndexedRowMatrix matrixA: $rowA by $colA ; matrixB: $rowB by $colB, " +
          s"m, k, n: $m, $k, $n; ${Calendar.getInstance().getTime}")
        val rowsA = sc.parallelize(0L until rowA, parallelism).map(row =>
          IndexedRow(row, Vectors.dense(Vector.rand[Double](colA).toArray)))
        val rowsB = sc.parallelize(0L until rowB, parallelism).map(row =>
          IndexedRow(row, Vectors.dense(Vector.rand[Double](colB).toArray)))
        val indMatA = new IndexedRowMatrix(rowsA, rowA, colA)
        val indMatB = new IndexedRowMatrix(rowsB, rowB, colB)
        val rowsPerBlockA = math.ceil(1.0 * rowA / m).toInt
        val colsPerBlockA = math.ceil(1.0 * colA / k).toInt

        val rowsPerBlockB = math.ceil(1.0 * rowB / k).toInt
        val colsPerBlockB = math.ceil(1.0 * colB / n).toInt
        val t0 = System.currentTimeMillis()
        val matA = indMatA.toBlockMatrix(rowsPerBlockA, colsPerBlockA)
        val matB = indMatB.toBlockMatrix(rowsPerBlockB, colsPerBlockB)
        val result = matA.multiply(matB)
        MTUtils.evaluate(result.blocks)
        println(s"MLlib RMM fom IndexedRowMatrix used time ${(System.currentTimeMillis() - t0)} millis " +
          s";${Calendar.getInstance().getTime}")
      case 4 =>
        val rowsPerBlockA = args(4).toInt
        val colsPerBlockA, rowsPerBlockB = args(5).toInt
        val colsPerBlockB = args(6).toInt
        val parallelism = 320
        println(s"MLlib RMM fom IndexedRowMatrix matrixA: $rowA by $colA ; matrixB: $rowB by $colB, " +
          s"rowsPerBlockA, colsPerBlockA, colsPerBlockB: $rowsPerBlockA, $colsPerBlockA, $colsPerBlockB;" +
          s" ${Calendar.getInstance().getTime}")
        val rowsA = sc.parallelize(0L until rowA, parallelism).map(row =>
          IndexedRow(row, Vectors.dense(Vector.rand[Double](colA).toArray)))
        val rowsB = sc.parallelize(0L until rowB, parallelism).map(row =>
          IndexedRow(row, Vectors.dense(Vector.rand[Double](colB).toArray)))
        val indMatA = new IndexedRowMatrix(rowsA, rowA, colA)
        val indMatB = new IndexedRowMatrix(rowsB, rowB, colB)
        val t0 = System.currentTimeMillis()
        val matA = indMatA.toBlockMatrix(rowsPerBlockA, colsPerBlockA)
        val matB = indMatB.toBlockMatrix(rowsPerBlockB, colsPerBlockB)
        val result = matA.multiply(matB)
        MTUtils.evaluate(result.blocks)
        println(s"MLlib RMM fom IndexedRowMatrix with direct rowsPerBlock used time ${(System.currentTimeMillis() - t0)} millis " +
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
