package edu.nju.pasalab.marlin.matrix

import breeze.linalg.{DenseMatrix => BDM}

/**
  * This object offers some local matrix multiplication algorithms
  */
object LibMatrixMult {

  /**
    * Matrix multiplication between dense matrix in breeze's `DenseMatrix` format and sparse matrix
    * @param denseMat
    * @param sparseMat
    */
  private[marlin] def multDenseSparse(denseMat: BDM[Double],
                                      sparseMat: SparseMatrix): BDM[Double] = {
    require(denseMat.cols == sparseMat.numRows,
      s"matrix dimension mismatch: ${denseMat.cols} v.s ${sparseMat.numRows}")
    val m = denseMat.rows
    val n = sparseMat.numCols
    val c = Array.ofDim[Double](m * n)
    var i, cix = 0
    while(i < n){
      val bcol = sparseMat.values(i)
      if(bcol != null && bcol.indices.size != 0){
        val bix = bcol.indices
        val bvals = bcol.values
        if(bvals.size == 1 && bvals(0) == 1.0){
          System.arraycopy(denseMat.data, bix(0) * m, c, cix, m)
        } else {
          for(k <- 0 until bix.size){
            for(j <- 0 until n)
            c(cix + j) += bvals(k) * denseMat.data( bix(k) * m + j)
          }
        }
      }
      i += 1
      cix += m
    }
    new BDM(m, n, c)
  }

}
