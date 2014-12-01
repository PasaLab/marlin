package edu.nju.pasalab.marlin.matrix

import edu.nju.pasalab.marlin.utils.LocalSparkContext
import org.apache.spark.rdd.RDD
import org.scalatest.FunSuite
import breeze.linalg.{DenseMatrix => BDM}

class BlockMatrixSuite extends FunSuite with LocalSparkContext{

  val m = 4
  val n = 4
  val r = 2
  val c = 2
  val blks = Seq(
    (new BlockID(0, 0), BDM((0.0, 1.0), (2.0, 3.0))),
    (new BlockID(0, 1), BDM((2.0, 3.0), (4.0, 5.0))),
    (new BlockID(1, 0), BDM((3.0, 2.0), (1.0, 1.0))),
    (new BlockID(1, 1), BDM((1.0, 0.0), (1.0, 1.0)))
  ).map( t => (t._1, t._2))
  val data = Seq(
    (0L, Vectors.dense(0.0, 1.0, 2.0, 3.0)),
    (1L, Vectors.dense(2.0, 3.0, 4.0, 5.0)),
    (2L, Vectors.dense(3.0, 2.0, 1.0, 0.0)),
    (3L, Vectors.dense(1.0, 1.0, 1.0, 1.0))
  ).map(t => (t._1, t._2))
  var indexRows: RDD[(Long, DenseVector)] = _
  var blocks: RDD[(BlockID, BDM[Double])] = _

  override protected def beforeAll() {
    super.beforeAll()
    indexRows = sc.parallelize(data, 2)
    blocks = sc.parallelize(blks, 2)
  }
  

  test("matrix size"){
    val mat = new BlockMatrix(blocks)
    assert(mat.numRows() === m)
    assert(mat.numCols() === n)
    assert(mat.numBlksByRow() === r)
    assert(mat.numBlksByCol() === c)
  }

  test("empty blocks") {
    val block = sc.parallelize(Seq[(BlockID, BDM[Double])](), 1)
    val mat = new BlockMatrix(block)
    intercept[RuntimeException] {
      mat.numRows()
    }
    intercept[RuntimeException] {
      mat.numCols()
    }
  }

  test("to Breeze local Matrix") {
    val mat = new BlockMatrix(blocks)
    val expected = BDM(
      (0.0, 1.0, 2.0, 3.0),
      (2.0, 3.0, 4.0, 5.0),
      (3.0, 2.0, 1.0, 0.0),
      (1.0, 1.0, 1.0, 1.0))
    assert(mat.toBreeze() === expected)
  }

  test("to DenseVecMatrix") {
    val mat = new BlockMatrix(blocks)
    val denVecMat = mat.toDenseVecMatrix()
    assert(mat.numRows() == denVecMat.numRows())
    assert(mat.numCols() == denVecMat.numCols())
    val rowSeq = denVecMat.rows.collect().toSeq
    assert(rowSeq.contains((0L, Vectors.dense(0.0, 1.0, 2.0, 3.0))))
    assert(rowSeq.contains((1L, Vectors.dense(2.0, 3.0, 4.0, 5.0))))
    assert(rowSeq.contains((2L, Vectors.dense(3.0, 2.0, 1.0, 0.0))))
    assert(rowSeq.contains((3L, Vectors.dense(1.0, 1.0, 1.0, 1.0))))
    /*
    assert(blkSeq.contains(new BlockID(0, 0), BDM((0.0, 1.0),(2.0, 3.0))))
    assert(blkSeq.contains(new BlockID(0, 1), BDM((2.0, 3.0),(4.0, 5.0))))
    assert(blkSeq.contains(new BlockID(1, 0), BDM((3.0, 2.0),(1.0, 1.0))))
    assert(blkSeq.contains(new BlockID(1, 1), BDM((1.0, 0.0),(1.0, 1.0))))
    */
  }

  test("Matrix-matrix and element-wise addition/subtract; element-wise multiply and divide"){
    val mat = new BlockMatrix(blocks)
    val denVecMat = new DenseVecMatrix(indexRows)
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
    assert(mat.add(denVecMat).toBreeze() === addSelf)
    assert(mat.subtract(1).toBreeze() === eleSubtract1)
    assert(mat.subtract(mat).toBreeze() === BDM.zeros[Double](4, 4))
    assert(mat.subtract(denVecMat).toBreeze() === BDM.zeros[Double](4, 4))
    assert(mat.multiply(2).toBreeze() === addSelf)
    assert(mat.divide(2).toBreeze() === divide2)
  }

  test("multiply a DenseVecMatrix") {
    val mat = new BlockMatrix(blocks)
    val denVecMat = new DenseVecMatrix(indexRows)
    val result = mat.multiply(denVecMat, 2)
    val blkSeq = result.blocks.collect().toSeq
    assert(blkSeq.contains(new BlockID(0, 0), BDM((11.0, 10.0),(23.0, 24.0))))
    assert(blkSeq.contains(new BlockID(0, 1), BDM((9.0,  8.0), (25.0, 26.0))))
    assert(blkSeq.contains(new BlockID(1, 0), BDM((7.0, 11.0), (6.0, 7.0))))
    assert(blkSeq.contains(new BlockID(1, 1), BDM((15.0, 19.0),(8.0, 9.0))))
  }

  test("multiply a BlockMatrix") {
    val mat = new BlockMatrix(blocks)
    val result = mat.multiply(mat, 2)
    val blkSeq = result.blocks.collect().toSeq
    assert(blkSeq.contains(new BlockID(0, 0), BDM((11.0, 10.0),(23.0, 24.0))))
    assert(blkSeq.contains(new BlockID(0, 1), BDM((9.0,  8.0), (25.0, 26.0))))
    assert(blkSeq.contains(new BlockID(1, 0), BDM((7.0, 11.0), (6.0, 7.0))))
    assert(blkSeq.contains(new BlockID(1, 1), BDM((15.0, 19.0),(8.0, 9.0))))
  }
  
  test("transpose"){
    val mat = new BlockMatrix(blocks)
    val result = mat.transpose()
    val blkSeq = result.blocks.collect().toSeq
    assert(blkSeq.contains(new BlockID(0, 0), BDM((0.0, 2.0), (1.0, 3.0))))
    assert(blkSeq.contains(new BlockID(0, 1), BDM((3.0, 1.0), (2.0, 1.0))))
    assert(blkSeq.contains(new BlockID(1, 0), BDM((2.0, 4.0), (3.0, 5.0))))
    assert(blkSeq.contains(new BlockID(1, 1), BDM((1.0, 1.0), (0.0, 1.0))))
  }

}
