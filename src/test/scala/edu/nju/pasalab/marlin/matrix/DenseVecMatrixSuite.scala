package edu.nju.pasalab.marlin.matrix

import edu.nju.pasalab.marlin.utils.LocalSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import breeze.linalg.{DenseMatrix => BDM}

class DenseVecMatrixSuite extends FunSuite with LocalSparkContext{

  val m = 4
  val n = 4
  val data = Seq(
    (0L, Vectors.dense(0.0, 1.0, 2.0, 3.0)),
    (1L, Vectors.dense(2.0, 3.0, 4.0, 5.0)),
    (2L, Vectors.dense(3.0, 2.0, 1.0, 0.0)),
    (3L, Vectors.dense(1.0, 1.0, 1.0, 1.0))
  ).map(t => IndexRow(t._1, t._2))
  val blks = Seq(
    (new BlockID(0, 0), BDM((0.0, 1.0), (2.0, 3.0))),
    (new BlockID(0, 1), BDM((2.0, 3.0), (4.0, 5.0))),
    (new BlockID(1, 0), BDM((3.0, 2.0), (1.0, 1.0))),
    (new BlockID(1, 1), BDM((1.0, 0.0), (1.0, 1.0)))
  ).map( t => (t._1, t._2))
  var indexRows: RDD[IndexRow] = _
  var blocks: RDD[(BlockID, BDM[Double])] = _

  override protected def beforeAll() {
    super.beforeAll()
    indexRows = sc.parallelize(data, 2)
    blocks = sc.parallelize(blks, 2)
  }
  

  test("matrix size"){
    val mat = new DenseVecMatrix(indexRows)
    assert(mat.numRows() === m)
    assert(mat.numCols() === n)
  }

  test("empty rows") {
    val rows = sc.parallelize(Seq[IndexRow](), 1)
    val mat = new DenseVecMatrix(rows)
    intercept[RuntimeException] {
      mat.numRows()
    }
    intercept[RuntimeException] {
      mat.numCols()
    }
  }

  test("to Breeze local Matrix") {
    val mat = new DenseVecMatrix(indexRows)
    val expected = BDM(
      (0.0, 1.0, 2.0, 3.0),
      (2.0, 3.0, 4.0, 5.0),
      (3.0, 2.0, 1.0, 0.0),
      (1.0, 1.0, 1.0, 1.0))
    assert(mat.toBreeze() === expected)
  }

  test("to BlockMatrix") {
    val mat = new DenseVecMatrix(indexRows)
    val blkMat = mat.toBlockMatrix(2, 2)
    assert(mat.numRows() == blkMat.numRows())
    assert(mat.numCols() == blkMat.numCols())
    val blkSeq = blkMat.blocks.collect().toSeq
    assert(blkSeq.contains(new BlockID(0, 0), BDM((0.0, 1.0),(2.0, 3.0))))
    assert(blkSeq.contains(new BlockID(0, 1), BDM((2.0, 3.0),(4.0, 5.0))))
    assert(blkSeq.contains(new BlockID(1, 0), BDM((3.0, 2.0),(1.0, 1.0))))
    assert(blkSeq.contains(new BlockID(1, 1), BDM((1.0, 0.0),(1.0, 1.0))))
  }

  test("multiply a DenseVecMatrix in CARMA-approach") {
    val mat = new DenseVecMatrix(indexRows)
    val result = mat.multiplyCarma(mat, 2)
    val blkSeq = result.blocks.collect().toSeq
    assert(blkSeq.contains(new BlockID(0, 0), BDM((11.0, 10.0), (23.0, 24.0), (7.0, 11.0), (6.0, 7.0))))
    assert(blkSeq.contains(new BlockID(0, 1), BDM((9.0,  8.0),  (25.0, 26.0), (15.0, 19.0),(8.0, 9.0))))
  }

  test("multiply a DenseVecMatrix in block-approach") {
    val mat = new DenseVecMatrix(indexRows)
    val result = mat.multiplyHama(mat, 2)
    val blkSeq = result.blocks.collect().toSeq
    assert(blkSeq.contains(new BlockID(0, 0), BDM((11.0, 10.0),(23.0, 24.0))))
    assert(blkSeq.contains(new BlockID(0, 1), BDM((9.0,  8.0), (25.0, 26.0))))
    assert(blkSeq.contains(new BlockID(1, 0), BDM((7.0, 11.0), (6.0, 7.0))))
    assert(blkSeq.contains(new BlockID(1, 1), BDM((15.0, 19.0),(8.0, 9.0))))
  }

  test("multiply a DenseVecMatrix, and select broadcast-approach") {
    val mat = new DenseVecMatrix(indexRows)
    val result = mat.multiply(mat, 2)
    val blkSeq = result.blocks.collect().toSeq
    assert(blkSeq.contains(new BlockID(0, 0), BDM((11.0, 10.0, 9.0,  8.0),(23.0, 24.0, 25.0, 26.0))))
    assert(blkSeq.contains(new BlockID(1, 0), BDM((7.0, 11.0, 15.0, 19.0),(6.0, 7.0, 8.0, 9.0))))
  }

}
