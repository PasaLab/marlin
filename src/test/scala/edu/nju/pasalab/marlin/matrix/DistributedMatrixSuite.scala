package edu.nju.pasalab.marlin.matrix

import edu.nju.pasalab.marlin.utils.{MarlinFunSuite, MTUtils, LocalSparkContext}
import org.apache.spark.rdd.RDD
import breeze.linalg.{ DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV }

import scala.collection.mutable.ArrayBuffer

class DistributedMatrixSuite extends MarlinFunSuite with LocalSparkContext {

  val m = 4
  val n = 4
  val r = 2
  val c = 2
  val data = Seq(
    (0L, BDV(0.0, 1.0, 2.0, 3.0)),
    (2L, BDV(3.0, 2.0, 1.0, 0.0)),
    (3L, BDV(1.0, 1.0, 1.0, 1.0)),
    (1L, BDV(2.0, 3.0, 4.0, 5.0))).map(t => (t._1, t._2))
  val blks = Seq(
    (new BlockID(0, 0), new SubMatrix(denseMatrix = BDM((0.0, 1.0), (2.0, 3.0)))),
    (new BlockID(0, 1), new SubMatrix(denseMatrix = BDM((2.0, 3.0), (4.0, 5.0)))),
    (new BlockID(1, 0), new SubMatrix(denseMatrix = BDM((3.0, 2.0), (1.0, 1.0)))),
    (new BlockID(1, 1), new SubMatrix(denseMatrix = BDM((1.0, 0.0), (1.0, 1.0))))).map(t => (t._1, t._2))
  var indexRows: RDD[(Long, BDV[Double])] = _
  var blocks: RDD[(BlockID, SubMatrix)] = _
  val sparseData = Seq(
    (0L, new BSV[Double](Array(1, 3), Array(2.0, 3.0), 4)),
    (1L, new BSV[Double](Array(0, 1), Array(5.0, 1.0),4)),
    (2L, new BSV[Double](Array(3), Array(2.0), 4)),
    (3L, new BSV[Double](Array(1, 2), Array(1.0, 1.0), 4))
  )
  var sparseRows: RDD[(Long, BSV[Double])] = _

  override def beforeAll() {
    super.beforeAll()
    indexRows = sc.parallelize(data, 2)
    blocks = sc.parallelize(blks, 2)
    sparseRows = sc.parallelize(sparseData, 2)
  }

  test("matrix size") {
    val mat = new DenseVecMatrix(indexRows)
    assert(mat.numRows() === m)
    assert(mat.numCols() === n)
    val ma = new BlockMatrix(blocks)
    assert(ma.numRows() === m)
    assert(ma.numCols() === n)
    assert(ma.numBlksByRow() === r)
    assert(ma.numBlksByCol() === c)
  }

  test("empty rows") {
    val rows = sc.parallelize(Seq[(Long, BDV[Double])](), 1)
    val mat = new DenseVecMatrix(rows)
    intercept[RuntimeException] {
      mat.numRows()
    }
    intercept[RuntimeException] {
      mat.numCols()
    }

    val block = sc.parallelize(Seq[(BlockID, SubMatrix)](), 1)
    val ma = new BlockMatrix(block)
    intercept[RuntimeException] {
      ma.numRows()
    }
    intercept[RuntimeException] {
      ma.numCols()
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

    val ma = new BlockMatrix(blocks)
    assert(ma.toBreeze() === expected)
  }

  test("to BlockMatrix") {
    val mat = new DenseVecMatrix(indexRows)
    val blkMat = mat.toBlockMatrix(2, 2)
    assert(mat.numRows() == blkMat.numRows())
    assert(mat.numCols() == blkMat.numCols())
    val blkSeq = blkMat.blocks.collect().toSeq.map{case(blkId, blk) => (blkId, blk.denseBlock)}
    assert(blkSeq.contains(BlockID(0, 0), BDM((0.0, 1.0), (2.0, 3.0))))
    assert(blkSeq.contains(BlockID(0, 1), BDM((2.0, 3.0), (4.0, 5.0))))
    assert(blkSeq.contains(BlockID(1, 0), BDM((3.0, 2.0), (1.0, 1.0))))
    assert(blkSeq.contains(BlockID(1, 1), BDM((1.0, 0.0), (1.0, 1.0))))
    val blkMat2 = mat.toBlockMatrix(1, 4)
    val matCB22 = mat.toBlockMatrixFromCoordinate(2, 2)
    val matCB14 = mat.toBlockMatrixFromCoordinate(1, 4)
    val expected = mat.toBreeze()
    assert(blkMat.toBreeze() === expected)
    assert(blkMat2.toBreeze() === expected)
    assert(matCB14.toBreeze() === expected)
    assert(matCB22.toBreeze() === expected)

  }


  test("to DenseVecMatrix") {
    val ma = new BlockMatrix(blocks)
    val denVecMat = ma.toDenseVecMatrix()
//    denVecMat.print()
    assert(ma.numRows() == denVecMat.numRows())
    assert(ma.numCols() == denVecMat.numCols())
    val rowSeq = denVecMat.rows.collect().toSeq
    assert(rowSeq.contains((0L, BDV(0.0, 1.0, 2.0, 3.0))))
    assert(rowSeq.contains((1L, BDV(2.0, 3.0, 4.0, 5.0))))
    assert(rowSeq.contains((2L, BDV(3.0, 2.0, 1.0, 0.0))))
    assert(rowSeq.contains((3L, BDV(1.0, 1.0, 1.0, 1.0))))
  }

  test("disVec to disVec") {
    val data = Seq(
      (0, BDV(0.0, 1.0, 2.0, 3.0)),
      (1, BDV(4.0, 5.0, 6.0, 7.0)),
      (2, BDV(8.0, 9.0, 10.0, 11.0)))
    // set partitionNum to 3, to make every one element as every partition
    val vec = sc.parallelize(data, 3)
    val disVec1 = new DistributedVector(vec)
    val splitStatus = Array.ofDim[ArrayBuffer[(Int, (Int, Int), (Int, Int))]](3)
    // create the splitsttus of each partition
    for (i <- splitStatus.indices) {
      val tmp = new ArrayBuffer[(Int, (Int, Int), (Int, Int))]()
      if (i == 0) {
        tmp.+=((0, (0, 2), (0, 2)), (1, (3, 3), (0, 0)))
      } else if(i == 1){
        tmp.+=((1, (0, 1), (1, 2)), (2, (2, 3), (0, 1)))
      } else {
        tmp.+=((2, (0, 0), (2, 2)), (3, (1, 3), (0, 2)))
      }
      splitStatus(i) = tmp
    }
    val disVec2 = disVec1.toDisVector(splitStatus, 4)
    assert(disVec1.toBreeze() === disVec2.toBreeze())
  }


  test("generate random sparse matrix"){
    val s = MTUtils.randomSpaVecMatrix(sc, 10, 8, 0.3)
    assert(s.nRows == 10L)
  }

  test("Sparse matrix multiply") {
    val sm = new SparseVecMatrix(sparseRows, 4L, 4L)
    val sm2 = new SparseVecMatrix(sparseRows, 4L, 4L)
    val result = BDM(
      (10.0, 5.0, 3.0, 0.0),
      (5.0, 11.0, 0.0, 15.0),
      (0.0, 2.0, 2.0, 0.0),
      (5.0, 1.0, 0.0, 2.0)
    )
    assert(sm.multiplySparse(sm2).toBreeze() === result)
  }

  test("Matrix-matrix and element-wise addition/subtract; element-wise multiply and divide") {
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
    val ma = new BlockMatrix(blocks)
    assert(ma.add(1).toBreeze() === eleAdd1)
    assert(ma.add(ma).toBreeze() === addSelf)
    assert(ma.add(mat).toBreeze() === addSelf)
    assert(ma.subtract(1).toBreeze() === eleSubtract1)
    assert(ma.subtract(ma).toBreeze() === BDM.zeros[Double](4, 4))
    assert(ma.subtract(mat).toBreeze() === BDM.zeros[Double](4, 4))
    assert(ma.multiply(2).toBreeze() === addSelf)
    assert(ma.divide(2).toBreeze() === divide2)
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

  test("DenseVecMatrix multiply a DenseVecMatrix, and select broadcast-approach") {
    val mat = new DenseVecMatrix(indexRows)
    val result = mat.multiply(mat, 2)
    val expected = BDM(
      (11.0, 10.0, 9.0, 8.0),
      (23.0, 24.0, 25.0, 26.0),
      (7.0, 11.0, 15.0, 19.0),
      (6.0, 7.0, 8.0, 9.0))
    assert(result.toBreeze() === expected)
  }

  test("new matrix multiplication") {
    val mat = new DenseVecMatrix(indexRows)
    val result = mat.multiply(mat, (2, 2, 1))
    val result2 = mat.multiply(mat, (2, 1, 2))
    val result3 = mat.multiply(mat, (2, 2, 2))
    val expected = BDM(
      (11.0, 10.0, 9.0, 8.0),
      (23.0, 24.0, 25.0, 26.0),
      (7.0, 11.0, 15.0, 19.0),
      (6.0, 7.0, 8.0, 9.0))
    assert(result.toBreeze() === expected)
    assert(result2.toBreeze() === expected)
    assert(result3.toBreeze() === expected)
  }

  test("DenseVecMatrix multiply a local matrix"){
    val mat = new DenseVecMatrix(indexRows)
    val local = BDM(
      (0.0, 1.0, 2.0, 3.0),
      (2.0, 3.0, 4.0, 5.0),
      (3.0, 2.0, 1.0, 0.0),
      (1.0, 1.0, 1.0, 1.0))
    val expected = BDM(
      (11.0, 10.0, 9.0, 8.0),
      (23.0, 24.0, 25.0, 26.0),
      (7.0, 11.0, 15.0, 19.0),
      (6.0, 7.0, 8.0, 9.0))
    val result = mat.multiply(local)
    val result2 = mat.multiply(local)
    assert(result.toBreeze() === expected)
    assert(result2.toBreeze() === expected)
  }

  test("multiply a BlockMatrix") {
    val mat = new DenseVecMatrix(indexRows)
    val blkMat = new BlockMatrix(blocks)
    val result = mat.multiply(blkMat, 2)
    val expected = BDM(
      (11.0, 10.0, 9.0, 8.0),
      (23.0, 24.0, 25.0, 26.0),
      (7.0, 11.0, 15.0, 19.0),
      (6.0, 7.0, 8.0, 9.0))
    assert(result.toBreeze() === expected)

    val ma = new BlockMatrix(blocks)
    val result2 = ma.multiply(ma)
    val blkSeq2 = result2.blocks.collect().toSeq.map{case(blkId, blk) => (blkId, blk.denseBlock)}
    assert(blkSeq2.contains(new BlockID(0, 0), BDM((11.0, 10.0), (23.0, 24.0))))
    assert(blkSeq2.contains(new BlockID(0, 1), BDM((9.0, 8.0), (25.0, 26.0))))
    assert(blkSeq2.contains(new BlockID(1, 0), BDM((7.0, 11.0), (6.0, 7.0))))
    assert(blkSeq2.contains(new BlockID(1, 1), BDM((15.0, 19.0), (8.0, 9.0))))
  }

  test("BlockMatrix multiply a DenseVecMatrix and choose to run broadcast") {
    val ma = new BlockMatrix(blocks)
    val denVecMat = new DenseVecMatrix(indexRows)
    val result = ma.multiply(denVecMat, 2)
    val expected = BDM(
      (11.0, 10.0, 9.0, 8.0),
      (23.0, 24.0, 25.0, 26.0),
      (7.0, 11.0, 15.0, 19.0),
      (6.0, 7.0, 8.0, 9.0))
    assert(result.toBreeze() === expected)
  }


  test("transpose") {
    val mat = new DenseVecMatrix(indexRows)
    val result = mat.transpose()
    val blkSeq = result.blocks.collect().toSeq.map{case(blkId, blk) => (blkId, blk.denseBlock)}
    assert(blkSeq.contains(new BlockID(0, 0), BDM((0.0, 2.0), (1.0, 3.0), (2.0, 4.0), (3.0, 5.0))))
    assert(blkSeq.contains(new BlockID(0, 1), BDM((3.0, 1.0), (2.0, 1.0), (1.0, 1.0), (0.0, 1.0))))

    val ma = new BlockMatrix(blocks)
    val result2 = ma.transpose()
    val blkSeq2 = result2.blocks.collect().toSeq.map{case(blkId, blk) => (blkId, blk.denseBlock)}
    assert(blkSeq2.contains(new BlockID(0, 0), BDM((0.0, 2.0), (1.0, 3.0))))
    assert(blkSeq2.contains(new BlockID(0, 1), BDM((3.0, 1.0), (2.0, 1.0))))
    assert(blkSeq2.contains(new BlockID(1, 0), BDM((2.0, 4.0), (3.0, 5.0))))
    assert(blkSeq2.contains(new BlockID(1, 1), BDM((1.0, 1.0), (0.0, 1.0))))
  }


  test("sum") {
    val mat = new DenseVecMatrix(indexRows)
    val blkMat = new BlockMatrix(blocks)
    assert(mat.sum() === 30.0)
    assert(blkMat.sum() === 30.0)
  }

  test("dot product") {
    val mat = new DenseVecMatrix(indexRows)
    val blkMat = new BlockMatrix(blocks)
    val dotProduct = BDM(
      (0.0, 1.0, 4.0, 9.0),
      (4.0, 9.0, 16.0, 25.0),
      (9.0, 4.0, 1.0, 0.0),
      (1.0, 1.0, 1.0, 1.0))
    assert(mat.dotProduct(mat).toBreeze() === dotProduct)
    assert(mat.dotProduct(blkMat).toBreeze() === dotProduct)
    assert(blkMat.dotProduct(mat).toBreeze() === dotProduct)
    assert(blkMat.dotProduct(blkMat).toBreeze() === dotProduct)
  }

  test("DenseVecMatrix inverse") {
    val row = Seq(
      (0L, BDV(0.0, 0.0, 1.0)),
      (1L, BDV(0.0, 1.0, 0.0)),
      (2L, BDV(1.0, 0.0, 0.0))).map(t => (t._1, t._2))
    val mat = new DenseVecMatrix(sc.parallelize(row, 2))
    val inverse = mat.inverse()
    val identity = BDM(
      (0.0, 0.0, 1.0),
      (0.0, 1.0, 0.0),
      (1.0, 0.0, 0.0))
    assert(inverse.toBreeze() === identity)
  }

  test("repeat by row and column") {
    val mat = new DenseVecMatrix(indexRows)
    val mat2ByRow = MTUtils.repeatByRow(mat, 2)
    val mat2ByCol = MTUtils.repeatByColumn(mat, 2)
    val expected1 = BDM(
      (0.0, 1.0, 2.0, 3.0, 0.0, 1.0, 2.0, 3.0),
      (2.0, 3.0, 4.0, 5.0, 2.0, 3.0, 4.0, 5.0),
      (3.0, 2.0, 1.0, 0.0, 3.0, 2.0, 1.0, 0.0),
      (1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0))
    assert(mat2ByRow.toBreeze() === expected1)
    val expected2 = BDM(
      (0.0, 1.0, 2.0, 3.0),
      (2.0, 3.0, 4.0, 5.0),
      (3.0, 2.0, 1.0, 0.0),
      (1.0, 1.0, 1.0, 1.0),
      (0.0, 1.0, 2.0, 3.0),
      (2.0, 3.0, 4.0, 5.0),
      (3.0, 2.0, 1.0, 0.0),
      (1.0, 1.0, 1.0, 1.0))
    assert(mat2ByCol.toBreeze() === expected2)
  }
  /*
   test("Matrix SVD"){
     val row = Seq(
     (0L, Vectors.dense(2.0, 0.0, 8.0, 6.0, 0.0)),
     (1L, Vectors.dense(1.0, 6.0, 0.0, 1.0, 7.0)),
     (2L, Vectors.dense(5.0, 0.0, 7.0, 4.0, 0.0)),
     (3L, Vectors.dense(7.0, 0.0, 8.0, 5.0, 0.0)),
     (4L, Vectors.dense(0.0, 10.0, 0.0, 0.0, 7.0))
     ).map(t => (t._1, t._2))
     val mat = new DenseVecMatrix(sc.parallelize(row,2))
     val svd = mat.computeSVD(3, true)
     assert(svd.s.toBreeze == Vectors.dense(17.92, 15.17, 3.56).toBreeze)
   }
   */

  test(" BLAS1 distributed vector multiplication") {
    val vectors = Seq(
      (0, BDV(1.0, 2.0)),
      (1, BDV(3.0, 4.0)))
    val disVec1 = new DistributedVector(sc.parallelize(vectors))
    val disVec2 = new DistributedVector(sc.parallelize(vectors))
    val expected = BDM(
      (1.0, 2.0, 3.0, 4.0),
      (2.0, 4.0, 6.0, 8.0),
      (3.0, 6.0, 9.0, 12.0),
      (4.0, 8.0, 12.0,16.0))
    val matResult = disVec1.multiply(disVec2.transpose())
    val doubleResult = disVec1.transpose().multiply(disVec2)
    val doubleResultLocal = disVec1.transpose().multiply(disVec2, "local")
    assert(matResult.right.get.toBreeze() === expected)
    assert(doubleResult.left.get === 30.0)
    assert(doubleResultLocal.left.get === 30.0)
  }



  test("BlockMatrix to BlockMatrix") {
    val mat = new DenseVecMatrix(indexRows)
    val blk1 = mat.toBlockMatrix(2, 2)
    val blk2 = blk1.toBlockMatrix(1, 4)
    val blk3 = blk1.toBlockMatrix(4, 1)
    assert(blk1.toBreeze() === blk2.toBreeze())
    assert(blk1.toBreeze() === blk3.toBreeze())
  }

  test("BlockMatrix multiply a BlockMatrix") {
    val mat = new DenseVecMatrix(indexRows)
    val blk1 = mat.toBlockMatrix(2, 2)
    val blk2 = mat.toBlockMatrix(1, 4)
    val m = blk1.toBlockMatrix(2, 1)
    val result = m.multiply(blk2)
    val expected = BDM(
      (11.0, 10.0, 9.0, 8.0),
      (23.0, 24.0, 25.0, 26.0),
      (7.0, 11.0, 15.0, 19.0),
      (6.0, 7.0, 8.0, 9.0))
    assert(result.toBreeze() === expected)
  }

  test("BlockMatrix multiply a broadcast matrix"){
    val blkMat = new BlockMatrix(blocks)
    val local = BDM(
      (0.0, 1.0, 2.0, 3.0),
      (2.0, 3.0, 4.0, 5.0),
      (3.0, 2.0, 1.0, 0.0),
      (1.0, 1.0, 1.0, 1.0))
    val expected = BDM(
      (11.0, 10.0, 9.0, 8.0),
      (23.0, 24.0, 25.0, 26.0),
      (7.0, 11.0, 15.0, 19.0),
      (6.0, 7.0, 8.0, 9.0))
    val result = blkMat.multiply(local)
    assert(result.toBreeze() === expected)
  }

}
