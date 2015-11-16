package edu.nju.pasalab.marlin.matrix

import breeze.linalg.{DenseMatrix => BDM}
import edu.nju.pasalab.marlin.utils.MarlinFunSuite

class LocalMatrixSuite extends MarlinFunSuite {

  test("sparse matrix to breeze `DenseMatrix`"){
    val spVectors = Array(new SparseVector(4, Array(1), Array(1.0)),
      new SparseVector(4, Array(0, 3), Array(2.0, 1.0)),
      new SparseVector(4, Array(0), Array(3.0)),
      new SparseVector(4, Array(2), Array(4.0)))
    val spMat = new SparseMatrix(4, 4, spVectors)
    val expected = BDM(
      (0.0, 2.0, 3.0, 0.0),
      (1.0, 0.0, 0.0, 0.0),
      (0.0, 0.0, 0.0, 4.0),
      (0.0, 1.0, 0.0, 0.0))
    assert(spMat.toBreeze.toDenseMatrix == expected)
  }

  test("sparse matrix multiply breeze `DenseMatrix`") {
    val spVectors = Array(new SparseVector(4, Array(1), Array(1.0)),
                          new SparseVector(4, Array(0, 3), Array(2.0, 1.0)),
                          new SparseVector(4, Array(0), Array(3.0)),
                          new SparseVector(4, Array(2), Array(4.0)))
    val spMat = new SparseMatrix(4, 4, spVectors)
    val expected = BDM(
      (2.0, 0.0, 0.0, 12.0),
      (0.0, 2.0, 3.0, 0.0),
      (0.0, 4.0, 0.0, 0.0),
      (1.0, 0.0, 0.0, 0.0))
    assert(spMat.multiply(spMat) === expected)
  }
}
