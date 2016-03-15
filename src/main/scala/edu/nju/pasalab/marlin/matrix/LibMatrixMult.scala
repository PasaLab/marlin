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
        val bix = bcol.indices.get
        val bvals = bcol.values.get
        if(bvals.size == 1 && bvals(0) == 1.0){
          System.arraycopy(denseMat.data, bix(0) * m, c, cix, m)
        } else {
          for(k <- 0 until bix.size){
            for(j <- 0 until m)
            c(cix + j) += bvals(k) * denseMat.data( bix(k) * m + j)
          }
        }
      }
      i += 1
      cix += m
    }
    new BDM(m, n, c)
  }

  private[marlin] def multSparseDense(sparseMat: SparseMatrix,
                                     denseMat: BDM[Double]): BDM[Double] = {
    require(sparseMat.numCols == denseMat.rows,
      s"matrix dimension mismatch: ${sparseMat.numCols} v.s ${denseMat.rows}")
    val m = sparseMat.numRows
    val n = denseMat.cols
    val c = Array.ofDim[Double](m * n)
    val blocksizeI, blocksizeK = 32
    val cd = sparseMat.numCols
    val a = sparseMat.values
    val b = denseMat.data

    for( bi <- 0 until n by blocksizeK) {
      for(bk <- 0 until cd by blocksizeI){
        val bimin = Math.min(n, bi + blocksizeK)
        val bklen = Math.min(cd, bk + blocksizeI) - bk
        for(i <- bi until bimin){
          val bixi = i * cd + bi
          val cixj = i * m + 0

          for( k <- 0 until bklen){
            val value = b(bixi + k)
            val avec = a(bk + k)
            val alen = avec.indices.get.size
            val aix = avec.indices.get
            val avals = avec.values.get
            for(j <- 0 until alen){
              c( cixj + aix(j)) += value * avals(j)
            }
          }
        }
      }
    }
    new BDM(m, n, c)
  }

}
