package edu.nju.pasalab.marlin.matrix

import breeze.linalg.{DenseMatrix => BDM}
import org.apache.spark.Logging

class SubMatrix() extends Serializable with Logging{

  private[marlin] var denseBlock: BDM[Double] = null

  private[marlin] var sparseBlock: SparseMatrix = null

  protected var sparse: Boolean = true

  protected var nonZeros: Long = 0L

  def this(denseMatrix: BDM[Double]) = {
    this()
    sparse = false
    denseBlock = denseMatrix
  }

  def this(spMatrix: SparseMatrix) = {
    this()
    sparseBlock = spMatrix
  }

  def rows :Int = {
    if(sparse){
      sparseBlock.numRows
    }else denseBlock.rows
  }

  def cols :Int = {
    if(sparse){
      sparseBlock.numCols
    }else denseBlock.cols
  }

  def isSparse = this.sparse

  def add(other: SubMatrix): SubMatrix = {
    (this.isSparse, other.isSparse) match {
      case (false, false) =>
        val res: BDM[Double] = this.denseBlock + other.denseBlock
        new SubMatrix(denseMatrix = res)
      case _ =>
        throw new IllegalArgumentException(s"Not supported add-operator between matrices of sparsity " +
          s"with ${this.isSparse} and ${other.isSparse}")
    }
  }

  def add(b: Double): SubMatrix = {
    if(this.isSparse){
      val spValues = this.sparseBlock.values.map(sv =>
        Vectors.sparse(sv.size, sv.indices.get, sv.values.get.map(t => t + b)).asInstanceOf[SparseVector])
      new SubMatrix(spMatrix = new SparseMatrix(this.rows, this.cols, spValues))
    }else new SubMatrix(denseMatrix = (this.denseBlock + b).asInstanceOf[BDM[Double]])
  }

  def subtract(other: SubMatrix): SubMatrix = {
    (this.isSparse, other.isSparse) match {
      case (false, false) =>
        val c: BDM[Double] = this.denseBlock - other.denseBlock
        new SubMatrix(denseMatrix = c)
      case _ =>
        throw new IllegalArgumentException(s"Not supported subtract-operator between matrices of sparsity " +
          s"with ${this.isSparse} and ${other.isSparse}")
    }
  }

  def subtract(b: Double): SubMatrix = {
    if(this.isSparse){
      val spValues = this.sparseBlock.values.map(sv =>
        Vectors.sparse(sv.size, sv.indices.get, sv.values.get.map(t => t - b)).asInstanceOf[SparseVector])
      new SubMatrix(spMatrix = new SparseMatrix(this.rows, this.cols, spValues))
    }else new SubMatrix(denseMatrix = (this.denseBlock - b).asInstanceOf[BDM[Double]])
  }

  def divide(b: Double): SubMatrix = {
    if(this.isSparse){
      val spValues = this.sparseBlock.values.map(sv =>
        Vectors.sparse(sv.size, sv.indices.get, sv.values.get.map(t => t / b)).asInstanceOf[SparseVector])
      new SubMatrix(spMatrix = new SparseMatrix(this.rows, this.cols, spValues))
    }else new SubMatrix(denseMatrix = (this.denseBlock / b).asInstanceOf[BDM[Double]])
  }

  def multiply(other: SubMatrix): SubMatrix = {
    (this.isSparse, other.isSparse) match {
      case (false, false) =>
        val c: BDM[Double] = this.denseBlock * other.denseBlock
        new SubMatrix(denseMatrix = c)
      case (true, true) =>
        val c: BDM[Double] = this.sparseBlock.multiply(other.sparseBlock)
        new SubMatrix(denseMatrix = c)
      case (false, true) =>
        val c: BDM[Double] = LibMatrixMult.multDenseSparse(this.denseBlock, other.sparseBlock)
        new SubMatrix(denseMatrix = c)
      case (true, false) =>
        val c: BDM[Double] = LibMatrixMult.multSparseDense(this.sparseBlock, other.denseBlock)
        new SubMatrix(denseMatrix = c)
      case _ =>
        throw new IllegalArgumentException(s"Not supported multiply-operator between matrices of sparsity " +
          s"with ${this.isSparse} and ${other.isSparse}")
    }
  }

  def multiply(other: BDM[Double]): SubMatrix = {
    this.isSparse match {
      case false =>
        val c: BDM[Double] = this.denseBlock * other
        new SubMatrix(denseMatrix = c)
      case true =>
        val c: BDM[Double] = LibMatrixMult.multSparseDense(this.sparseBlock, other)
        new SubMatrix(denseMatrix = c)
      case _ =>
        throw new IllegalArgumentException(s"Not supported multiply-operator between matrices of sparsity " +
          s"with ${this.isSparse} and breeze BDM[Double]")
    }
  }

  def multiply(b: Double): SubMatrix = {
    if(this.isSparse){
      val spValues = this.sparseBlock.values.map(sv =>
        Vectors.sparse(sv.size, sv.indices.get, sv.values.get.map(t => t * b)).asInstanceOf[SparseVector])
      new SubMatrix(spMatrix = new SparseMatrix(this.rows, this.cols, spValues))
    }else {
      new SubMatrix(denseMatrix = (this.denseBlock * b).asInstanceOf[BDM[Double]])
    }
  }

  def multiply(v: Vector): DenseVector = {
    (this.isSparse, v.isInstanceOf[SparseVector]) match {
      case (false, false) =>
        new DenseVector(this.denseBlock * v.asInstanceOf[DenseVector].inner.get)
      case _ =>
        throw new IllegalArgumentException(s"Not supported multiply-operator between matrices sparsity " +
          s"with ${this.isSparse} and vector: ${v.getClass}")
    }
  }
}
