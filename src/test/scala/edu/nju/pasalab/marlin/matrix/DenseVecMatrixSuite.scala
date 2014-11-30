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
  ).map(t => (t._1, t._2))
  val blks = Seq(
    (new BlockID(0, 0), BDM((0.0, 1.0), (2.0, 3.0))),
    (new BlockID(0, 1), BDM((2.0, 3.0), (4.0, 5.0))),
    (new BlockID(1, 0), BDM((3.0, 2.0), (1.0, 1.0))),
    (new BlockID(1, 1), BDM((1.0, 0.0), (1.0, 1.0)))
  ).map( t => (t._1, t._2))
  var indexRows: RDD[(Long, DenseVector)] = _
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
    val rows = sc.parallelize(Seq[(Long, DenseVector)](), 1)
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

  test("Matrix-matrix and element-wise addition/subtract; element-wise multiply and divide"){
    val mat = new DenseVecMatrix(indexRows)
    val eleAdd1 = BDM(
      (1.0, 2.0, 3.0, 4.0),
      (3.0, 4.0, 5.0, 6.0),
      (4.0, 3.0, 2.0, 1.0),
      (2.0, 2.0, 2.0, 2.0))

    val addSelf = BDM(
      (0.0, 2.0, 4.0, 6.0),
      (4.0, 6.0, 8.0, 10.0),
      (6.0, 4.0, 2.0, 0.0),
      (2.0, 2.0, 2.0, 2.0))

    val eleSubtract1 = BDM(
      (-1.0, 0.0, 1.0, 2.0),
      (1.0, 2.0, 3.0, 4.0),
      (2.0, 1.0, 0.0, -1.0),
      (0.0, 0.0, 0.0, 0.0))

    val divide2 = BDM(
      (0.0, 0.5, 1.0, 1.5),
      (1.0, 1.5, 2.0, 2.5),
      (1.5, 1.0, 0.5, 0.0),
      (0.5, 0.5, 0.5, 0.5))

    assert(mat.add(1).toBreeze() === eleAdd1)
    assert(mat.add(mat).toBreeze() === addSelf)
    assert(mat.subtract(1).toBreeze() === eleSubtract1)
    assert(mat.subtract(mat).toBreeze() === BDM.zeros[Double](4, 4))
    assert(mat.multiply(2).toBreeze() === addSelf)
    assert(mat.divide(2).toBreeze() === divide2)
  }

  test("slice the matrix") {
    val mat = new DenseVecMatrix(indexRows)
    val sliceRow1To2 = BDM(
      (2.0, 3.0, 4.0, 5.0),
      (3.0, 2.0, 1.0, 0.0))
    val sliceCol1To2 = BDM(
      (1.0, 2.0),
      (3.0, 4.0),
      (2.0, 1.0),
      (1.0, 1.0))
    val sub1212 = BDM(
      (3.0, 4.0),
      (2.0, 1.0))
    assert(mat.sliceByRow(1, 2).toBreeze() === sliceRow1To2)
    assert(mat.sliceByColumn(1, 2).toBreeze() === sliceCol1To2)
    assert(mat.getSubMatrix(1, 2, 1, 2).toBreeze() === sub1212)
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

  test("multiply a BlockMatrix"){
    val mat = new DenseVecMatrix(indexRows)
    val blkMat = new BlockMatrix(blocks)
    val result = mat.multiply(blkMat, 2)
    val blkSeq = result.blocks.collect().toSeq
    assert(blkSeq.contains(new BlockID(0, 0), BDM((11.0, 10.0, 9.0,  8.0),(23.0, 24.0, 25.0, 26.0))))
    assert(blkSeq.contains(new BlockID(1, 0), BDM((7.0, 11.0, 15.0, 19.0),(6.0, 7.0, 8.0, 9.0))))
  }

  test("transpose"){
    val mat = new DenseVecMatrix(indexRows)
    val result = mat.transpose()
    val blkSeq = result.blocks.collect().toSeq
    assert(blkSeq.contains(new BlockID(0, 0), BDM((0.0, 2.0), (1.0, 3.0),(2.0, 4.0), (3.0, 5.0))))
    assert(blkSeq.contains(new BlockID(0, 1), BDM((3.0, 1.0), (2.0, 1.0),(1.0, 1.0), (0.0, 1.0))))
  }

  test("lu decompose"){
    val row = Seq(
      (0L, Vectors.dense(1.0, 2.0, 3.0)),
      (1L, Vectors.dense(4.0, 5.0, 6.0)),
      (2L, Vectors.dense(7.0, 8.0, 0.0))
    ).map(t => (t._1, t._2))
    val mat = new DenseVecMatrix(sc.parallelize(row,2))
    val (l, u) = mat.luDecompose()
//    l.print()
//    u.print()
    val result = l.multiply(u, 4).toBreeze()
//    println(result.toString())
//    result.print()
    val self = BDM(
      (1.0, 2.0, 3.0),
      (4.0, 5.0, 6.0),
      (7.0, 8.0, 0.0))
   assert(result === self)
  }



}
