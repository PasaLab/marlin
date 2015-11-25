package edu.nju.pasalab.marlin.matrix

import java.io.IOException
import java.util.Arrays
import java.util.Calendar
import edu.nju.pasalab.marlin.ml.ALSHelp
import edu.nju.pasalab.marlin.rdd.MatrixMultPartitioner
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.annotation.Experimental
import org.apache.spark.sql.types.{LongType, StructType, DoubleType, StructField}
import org.apache.spark.sql.{DataFrame, Row, SQLContext}

import scala.collection.mutable
import scala.collection.mutable.{ArrayBuffer, ArrayBuilder}
import scala.collection.parallel.mutable.ParArray

import org.apache.hadoop.io.{Text, NullWritable}
import org.apache.hadoop.mapred.TextOutputFormat
import org.apache.log4j.{Logger, Level}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.spark.HashPartitioner
import org.apache.spark.storage.StorageLevel

import edu.nju.pasalab.marlin.utils.MTUtils

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, SparseVector => BSV,
axpy => brzAxpy, svd => brzSvd, LU => brzLU, inv => brzInv, cholesky => brzCholesky, Transpose, upperTriangular, lowerTriangular}
import breeze.numerics.{sqrt => brzSqrt}
import com.github.fommil.netlib.BLAS.{getInstance => blas}

import com.github.fommil.netlib.ARPACK
import org.netlib.util.{intW, doubleW}

import scala.language.implicitConversions
import scala.util.Random

class DenseVecMatrix(
                      private[marlin] val rows: RDD[(Long, BDV[Double])],
                      private var nRows: Long,
                      private var nCols: Long) extends DistributedMatrix with Logging {

  private var resultCols: Long = 0

  def this(rows: RDD[(Long, BDV[Double])]) = this(rows, 0L, 0)

  def this(sc: SparkContext, array: Array[Array[Double]], partitions: Int = 2) {
    this(sc.parallelize(array.zipWithIndex.
      map { case (t, i) => (i.toLong, BDV(t)) }, partitions))
  }

  override def numCols(): Long = {
    if (nCols <= 0) {
      // Calling `first` will throw an exception if `rows` is empty.
      nCols = rows.first()._2.size
    }
    nCols
  }

  override def numRows(): Long = {
    if (nRows <= 0L) {
      // Reduce will throw an exception if `rows` is empty.
      nRows = rows.map(_._1).reduce(math.max) + 1L
    }
    nRows
  }

  def getRows = rows

  /** Collects data and assembles a local dense breeze matrix (for test only). */
  override private[marlin] def toBreeze(): BDM[Double] = {
    val m = numRows().toInt
    val n = numCols().toInt
    val mat = BDM.zeros[Double](m, n)
    rows.collect().foreach {
      case (rowIndex, vector) =>
        val i = rowIndex.toInt
        mat(i, ::) := vector.t
    }
    mat
  }

  /** Get the numbers of cores across the cluster */
  private[marlin] def getClusterCores(): Int = {
    val sc = rows.context
    val cores = if (!sc.getConf.getOption("spark.default.parallelism").isEmpty) {
      sc.getConf.get("spark.default.parallelism").toInt
    } else {
      sc.defaultMinPartitions
    }
    cores
  }

  /**
   * This function is used to satisfy the
   * @param other
   * @param cores
   * @return
   */
  def multiply(other: DistributedMatrix, cores: Int): DistributedMatrix = {
    multiply(other, cores, 300)
  }


  // better transformation 
  def multiply(other: DistributedMatrix, splitMode: (Int, Int, Int)): BlockMatrix = {
    require(numCols() == other.numRows(), s"Dimension mismatch " +
      s"during matrix-matrix multiplication: ${numCols()} vs ${other.numRows()}")

    val (m, k, n) = splitMode
    val partitioner = new MatrixMultPartitioner(m, k, n)
    other match {
      case that: DenseVecMatrix =>
        val thisEmits = toBlocks(m, k, n, "right").partitionBy(partitioner)
        val otherEmits = that.toBlocks(m, k, n, "left").partitionBy(partitioner)
        val result = if (k == 1) {
          thisEmits.join(otherEmits).mapPartitions(iter =>
            iter.map { case (blkId, (block1, block2)) =>
              val c: BDM[Double] = block1.asInstanceOf[BDM[Double]] * block2.asInstanceOf[BDM[Double]]
              (BlockID(blkId.row, blkId.column), new SubMatrix(denseMatrix = c))
            }
          )
        } else {
          thisEmits.join(otherEmits).mapPartitions(iter =>
            iter.map { case (blkId, (block1, block2)) =>
              val c: BDM[Double] = block1.asInstanceOf[BDM[Double]] * block2.asInstanceOf[BDM[Double]]
              (BlockID(blkId.row, blkId.column), new SubMatrix(denseMatrix = c))
            }
          ).reduceByKey((a, b) => a.add(b))
        }
        new BlockMatrix(result, numRows(), that.numCols(), m, n)

      case that: BlockMatrix =>
        val thisEmits = this.toBlockMatrix(m, k)
        val thatEmits = that.toBlockMatrix(m, k)
        thisEmits.multiply(thatEmits)
    }
  }


  /**
   * distributed matrix-vector multiply, here I use customized split mode
   * @param vector
   * @param splitMode
   */
  def multiply(vector: DistributedVector, splitMode: (Int, Int)): DistributedVector = {
    require(numCols() == vector.length, s"Dimension mismatch " +
      s"during matrix-matrix multiplication: ${numCols()} vs ${vector.length}")
    val (m, k) = splitMode
    toBlockMatrix(m, k).multiply(vector)
  }


  /**
   * distributed matrix multiply a local vector, here I use customized split mode
   * @param vector
   * @param splitMode
   */
  def multiply(vector: BDV[Double], splitMode: Int): DistributedVector = {
    val m = splitMode
    toBlockMatrix(m, 1).multiply(vector)
  }

  /**
   * multiply a local vector without transform the DenseVecMatrix to BlockMatrix
   * @param vector
   */
  def multiply(vector: BDV[Double]): BDV[Double] = {
    val bvec = rows.context.broadcast(vector)
    val data = rows.mapPartitions { iter =>
      val vec = bvec.value
      iter.map { case (id, v) =>
        (id.toInt, v.t * vec)
      }
    }.collect()
    val result = BDV.zeros[Double](data.length)
    for ((id, v) <- data) {
      result(id) = v
    }
    result
  }


  /**
   * Matrix-matrix multiply
   *
   * @param other another matrix
   * @param cores the real num of cores you set in the environment
   * @param broadcastThreshold the threshold of broadcasting variable, default num is 300 MB,
   *                           user can set it, the unit of this parameter is MB
   * @return result in BlockMatrix type
   */
  def multiply(other: DistributedMatrix,
               cores: Int,
               broadcastThreshold: Int = 300): DistributedMatrix = {
    require(numCols == other.numRows(),
      s"Dimension mismatch during matrix-matrix multiplication: ${numCols()} vs ${other.numRows()}")
    other match {
      case that: DenseVecMatrix =>
        val broadcastSize = broadcastThreshold * 1024 * 1024 / 8
        if (that.numRows() * that.numCols() <= broadcastSize) {
          multiply(that.toBreeze())
        } else if (numRows() * numCols() <= broadcastSize) {
          that.multiply(this.toBreeze())
        } else if (0.8 < (numRows() * that.numCols()).toDouble / (numCols() * numCols()).toDouble
          && (numRows() * that.numCols()).toDouble / (numCols() * numCols()).toDouble < 1.2
          && numRows() / numCols() < 1.2
          && numRows() / numCols() > 0.8) {
          val split = math.floor(math.pow(3 * cores, 1.0 / 3.0)).toInt
          multiply(that, (split, split, split))
        } else {
          val splitMethod =
            MTUtils.splitMethod(numRows(), numCols(), other.numCols(), cores)
          multiply(that, splitMethod)
        }
      case that: BlockMatrix =>
        val broadSize = broadcastThreshold * 1024 * 1024 / 8
        if (that.numRows() * that.numCols() <= broadSize) {
          this.multiply(that.toBreeze())
        } else if (this.numRows() * this.numCols() <= broadSize) {
          that.multiplyBy(this.toBreeze())
        } else {
          val splitMethod =
            MTUtils.splitMethod(numRows(), numCols(), other.numCols(), cores)
          multiply(that, splitMethod)
        }
    }
  }

  /**
   * Column bind from two matrices, and create a new matrix, just like cBind in R
   * @param other another matrix in DenseVecMatrix format
   * @return
   */
  def cBind(other: DistributedMatrix): DistributedMatrix = {
    require(numRows() == other.numRows(), s"Dimension mismatch:  ${numRows()} vs ${other.numRows()}")
    other match {
      case that: DenseVecMatrix =>
        val result = rows.join(that.rows).mapValues{case(v1, v2) =>
          BDV(v1.toArray ++: v2.toArray)
        }
        new DenseVecMatrix(result, numRows(), numCols() + that.numCols())
      case that: BlockMatrix =>
        val thatDenVec = that.toDenseVecMatrix()
        cBind(thatDenVec)
      case that: DistributedMatrix =>
        throw new IllegalArgumentException("Do not support this type " + that.getClass + " for cBind operation")
    }
  }


  /**
   * multiply a elementary matrix on the left to apply row switching transformations
   *
   * @param permutation the elementary row switching matrix
   * @return the multiplication
   */
  private[marlin] def rowExchange(permutation: Array[Int]): DenseVecMatrix = {
    // val permPair = (0 until permutation.length).toArray.zip(permutation)
    require(numRows == permutation.length,
      s"Dimension mismatch, row permutation matrix: ${permutation.length} vs $nRows")
    val index = rows.context.parallelize(permutation.zipWithIndex.toSeq, getClusterCores())
      .map(t => (t._1.toLong, t._2.toLong))
    val result = rows.join(index).map{case(id1, (v, id2)) => (id2, v)}
    new DenseVecMatrix(result, numRows(), numCols())
  }

  def inverse(): BlockMatrix = {
    inverse("auto")
  }

  /**
   * This is an experimental implementation of block lu decomposition. The method is still in progress.
   * LU decompose this DenseVecMatrix to generate a lower triangular matrix L abd a upper matrix U
   * where A = L * U, BlockMatrix in the result contains the lower part and the upper part, and
   * Array[Int] means the permutation array.
   *
   * @param mode  in which manner should the result be calculated, locally or distributed
   */
  def luDecompose(mode: String = "auto"): (BlockMatrix, Array[Int]) = {
    require(numRows() == numCols(),
      s"LU decompose only support square matrix: ${numRows()} v.s ${numCols()}")
    object LUmode extends Enumeration {
      val LocalBreeze, DistSpark = Value
    }
    val computeMode = mode match {
      case "auto" => if (numRows > 6000L) {
        LUmode.DistSpark
      } else {
        LUmode.LocalBreeze
      }
      case "breeze" => LUmode.LocalBreeze
      case "dist" => LUmode.DistSpark
      case _ => throw new IllegalArgumentException(s"Do not support mode $mode.")
    }
    val (luResult: BlockMatrix, perm: Array[Int]) = computeMode match {
      case LUmode.LocalBreeze =>
        val brz = toBreeze()
        val lu = brzLU(brz)
        val pArray = (0 until lu._2.length).toArray
        for (i <- 0 until lu._2.length) {
          val tmp = pArray(i)
          pArray(i) = pArray(lu._2(i) - 1)
          pArray(lu._2(i) - 1) = tmp
        }
        val blk = rows.context.parallelize(Seq((BlockID(0, 0), new SubMatrix(denseMatrix = lu._1))), 1)
        (new BlockMatrix(blk, lu._1.rows, lu._1.cols, 1, 1), pArray)

      case LUmode.DistSpark =>
        val subMatrixBaseSize = rows.context.getConf.getInt("marlin.lu.basesize", 1000)
        val numBlksByRow, numBlksByCol = math.ceil(numRows().toDouble / subMatrixBaseSize.toDouble).toInt
        val subMatrixBase = math.ceil(numRows().toDouble / numBlksByRow.toDouble).toInt
        val pArray = Array.ofDim[Int](numRows().toInt)

        val partitioner = new HashPartitioner(2 * getClusterCores())
        var blkMat = this.toBlockMatrix(numBlksByRow, numBlksByCol).blocks //.partitionBy(partitioner)

        blkMat.cache()
        println("numBlkByRow is: " + numBlksByRow)
        println("original partitioner: " + partitioner.toString())
        val scatterRdds = Array.ofDim[RDD[(BlockID, SubMatrix)]](numBlksByRow - 1, 3)

        for (i <- 0 until numBlksByRow) {
          val matFirst = blkMat.filter(block => block._1.row == i && block._1.column == i)
          if (i == numBlksByRow - 1) {
            val (lu, p) = brzLU(matFirst.collect().head._2.denseBlock)
            val perm = (0 until p.length).toArray
            for (i <- 0 until perm.length) {
              val tmp = perm(i)
              perm(i) = perm(p(i) - 1)
              perm(p(i) - 1) = tmp
            }
            for (j <- 0 until perm.length) {
              pArray(i * subMatrixBase + j) = i * subMatrixBase + perm(j)
            }
            blkMat = matFirst.mapValues(block => new SubMatrix(denseMatrix = lu))
          } else {
            val matSecond = blkMat.filter(block => block._1.row == i && block._1.column > i)
            val matThird = blkMat.filter(block => block._1.row > i && block._1.column == i)
            val matForth = blkMat.filter(block => block._1.column > i && block._1.row > i)

            val bdata = rows.context.broadcast(matFirst.collect().head._2.denseBlock)

            logInfo(s"LU iteration: $i")
            val t0 = System.currentTimeMillis()
            val (mat, p) = brzLU(bdata.value)
            logInfo(s"in iteration $i, in master lu takes ${(System.currentTimeMillis() - t0) / 1000} seconds")

            val l = breeze.linalg.lowerTriangular(mat)
            for (i <- 0 until l.rows) {
              l.update(i, i, 1.0)
            }

            val perm = (0 until p.length).toArray
            for (i <- 0 until perm.length) {
              val tmp = perm(i)
              perm(i) = perm(p(i) - 1)
              perm(p(i) - 1) = tmp
            }

            for (j <- 0 until perm.length) {
              pArray(i * subMatrixBase + j) = i * subMatrixBase + perm(j)
            }
            val blup = rows.context.broadcast((l, breeze.linalg.upperTriangular(mat), perm))

            scatterRdds(i)(0) = matFirst.cache()
            scatterRdds(i)(1) = matSecond.mapValues(block => {
              val p = blup.value._3
              val l = blup.value._1
              val permutation = BDM.zeros[Double](p.length, p.length)
              for (j <- 0 until p.length) {
                permutation.update(j, p(j), 1.0)
              }
              val t0 = System.currentTimeMillis()
              val tmp: BDM[Double] = (l \ permutation) * block.denseBlock
              logInfo(s"in iteration $i, computation for A2 takes ${(System.currentTimeMillis() - t0) / 1000} seconds")
              new SubMatrix(denseMatrix = tmp)
            }).cache()
            scatterRdds(i)(2) = matThird.mapValues(block => {
              val t0 = System.currentTimeMillis()
              val tmp: BDM[Double] = block.denseBlock * brzInv(blup.value._2)
              logInfo(s"in iteration $i, computation for A3 takes ${(System.currentTimeMillis() - t0) / 1000} seconds")
              new SubMatrix(denseMatrix = tmp)
            }).cache()

            val nSplitNum, mSplitNum = numBlksByRow - (i + 1)
            val kSplitNum = 1

            val matThirdEmit = matThird
              .mapPartitions({
                iter =>
                  iter.flatMap { case (blkId, blk) =>
                    val array = Array.ofDim[(BlockID, BDM[Double])](nSplitNum)
                    for (j <- 0 until nSplitNum) {
                      //val seq = t._1.row * nSplitNum * kSplitNum + (j+i+1) * kSplitNum + t._1.column
                      val seq = 0
                      array(j) = (BlockID(blkId.row, (j + i + 1), seq), blk.denseBlock)
                    }
                    array
                  }
              }) //.partitionBy(partitioner)

            val matSecondEmit = matSecond
              .mapPartitions({
                iter =>
                  iter.flatMap { case (blkId, blk) =>
                    val array = Array.ofDim[(BlockID, BDM[Double])](mSplitNum)
                    for (j <- 0 until mSplitNum) {
                      val seq = 0
                      array(j) = (BlockID(j + i + 1, blkId.column, seq), blk.denseBlock)
                    }
                    array
                  }
              }) //.partitionBy(partitioner)
            println("matThirdEmit and matSecondEmit same partitioner? "
              + (matSecondEmit.partitioner == matThirdEmit.partitioner).toString)

            val mult = matThirdEmit.join(matSecondEmit, partitioner)
              .mapValues { case (blk1, blk2) =>
               new SubMatrix(denseMatrix = (blk1.asInstanceOf[BDM[Double]] * (bdata.value \ blk2.asInstanceOf[BDM[Double]]))
                 .asInstanceOf[BDM[Double]])
              } //.partitionBy(partitioner)
            blkMat = matForth
              .join(mult, partitioner).mapValues{case(a,b) => a.subtract(b)} //.partitionBy(partitioner).cache()
              .cache()
          }
        }

        for (i <- 0 until numBlksByRow - 1) {
          for (j <- 0 until 3)
            blkMat = blkMat.union(scatterRdds(i)(j))
        }
        //blkMat.partitionBy(partitioner)

        val bpArray = blkMat.context.broadcast(pArray)
        blkMat = blkMat.mapPartitions(iter =>
          iter.map { case (blkId, blk) =>
            if (blkId.row > blkId.column) {
              val array = bpArray.value
                .slice(subMatrixBase * blkId.row,
                  if (blkId.row == numBlksByRow - 1) {
                    numRows().toInt
                  }
                  else {
                    subMatrixBase * blkId.row + subMatrixBase
                  })

              val permutation = BDM.zeros[Double](array.length, array.length)
              for (j <- 0 until array.length) {
                permutation.update(j, array(j) - subMatrixBase * blkId.row, 1.0)
              }
              (blkId, new SubMatrix(denseMatrix =permutation).multiply(blk))
            } else {
              (blkId, blk)
            }

          }, true).cache()
        val result = new BlockMatrix(blkMat, numRows(), numCols(), numBlksByRow, numBlksByCol)
        //  println("cnt:" + result.blocks.count())
        (result, pArray)
    }
    (luResult, perm)
  }

  /**
   * This function is still in progress.
   * get the result of cholesky decomposition of this DenseVecMatrix
   *
   * @param mode  in which manner should the result be calculated, locally or distributed
   * @return matrix A, where A * A' = Matrix
   */
  def choleskyDecompose(mode: String = "auto"): BlockMatrix = {
    require(numRows() == numCols(),
      s"LU decompose only support square matrix: ${numRows()} v.s ${numCols()}")
    object LUmode extends Enumeration {
      val LocalBreeze, DistSpark = Value
    }
    val computeMode = mode match {
      case "auto" => if (numRows > 6000L) {
        LUmode.DistSpark
      } else {
        LUmode.LocalBreeze
      }
      case "breeze" => LUmode.LocalBreeze
      case "dist" => LUmode.DistSpark
      case _ => throw new IllegalArgumentException(s"Do not support mode $mode.")
    }

    val (luResult: BlockMatrix) = computeMode match {
      case LUmode.LocalBreeze =>
        val brz = toBreeze()
        val l = brzCholesky(brz)
        val blk = rows.context.parallelize(Seq((BlockID(0, 0), new SubMatrix(denseMatrix = l))), 1)
        new BlockMatrix(blk, l.rows, l.cols, 1, 1)
      case LUmode.DistSpark =>
        val subMatrixBaseSize = rows.context.getConf.getInt("marlin.cholesky.basesize", 1000)
        val numBlksByRow, numBlksByCol = math.ceil(numRows().toDouble / subMatrixBaseSize.toDouble).toInt
        val subMatrixBase = math.ceil(numRows().toDouble / numBlksByRow.toDouble).toInt

        val partitioner = new HashPartitioner(2 * getClusterCores())
        var blkMat = this.toBlockMatrix(numBlksByRow, numBlksByCol).blocks.partitionBy(partitioner)
        blkMat.cache()

        println("numBlkByRow is: " + numBlksByRow)
        println("original partitioner: " + partitioner.toString)
        val scatterRdds = Array.ofDim[RDD[(BlockID, SubMatrix)]](numBlksByRow - 1, 2)

        for (i <- 0 until numBlksByRow) {
          if (i == numBlksByRow - 1) {
            blkMat = blkMat.mapValues(block => new SubMatrix(denseMatrix = brzCholesky(block.denseBlock)))
          } else {
            logInfo(s"LU iteration: $i")
            val blksFirst = blkMat.filter(block => block._1.row == i && block._1.column == i)
            val blksThird = blkMat.filter(block => block._1.row > i && block._1.column == i)
            val blksForth = blkMat.filter(block => block._1.column > i && block._1.row >= block._1.column)

            val mat = blksFirst.collect().head._2.denseBlock
            for (j <- 0 until mat.rows)
              for (k <- 0 until j)
                mat(j, k) = mat(k, j)

            val l = brzCholesky(mat)
            val bltinverse = rows.context.broadcast(brzInv(l.t))
            scatterRdds(i)(0) = blksFirst.mapValues(block =>  new SubMatrix(denseMatrix = l)).cache()
            scatterRdds(i)(1) = blksThird.mapValues(block =>
              block.multiply(bltinverse.value)).cache()

            val nSplitNum = numBlksByRow - (i + 1)
            val mult = scatterRdds(i)(1)
              .mapPartitions({
                iter =>
                  iter.flatMap{case(blkId, blk) =>
                    val array = Array.ofDim[(BlockID, BDM[Double])](nSplitNum + 1)
                    for (j <- 0 until array.length) {
                      if (j < blkId.row - i) {
                        array(j) = (BlockID(blkId.row, j + i + 1, 0), blk.denseBlock)
                      } else {
                        array(j) = (BlockID(i + j, blkId.row, 0), blk.denseBlock.t)
                      }
                    }
                    array
                  }
              }).reduceByKey(partitioner, (a, b) => if (a.isTranspose) b * a else a * b)

            blkMat = blksForth.join(mult, partitioner).mapValues{case(a, b) =>
              a.subtract(new SubMatrix(denseMatrix = b))}.cache()
          }
        }
        for (i <- 0 until numBlksByRow - 1) {
          for (j <- 0 until 2)
            blkMat = blkMat.union(scatterRdds(i)(j))
        }
        //blkMat.partitionBy(partitioner)
        new BlockMatrix(blkMat)
    }
    println("row:" + luResult.blocks.count())
    luResult
  }

  /**
   * get the inverse of a square matrix
   * @param mode  in which manner should the inverse be calculated, locally or distributed
   * @return
   */
  def inverse(mode: String = "auto"): (BlockMatrix) = {
    require(numRows() == numCols(),
      s"Inversion only support square matrix: ${numRows()} v.s ${numCols()}")
    object LUmode extends Enumeration {
      val LocalBreeze, DistSpark = Value
    }
    val computeMode = mode match {
      case "auto" => if (numRows > 6000L) {
        LUmode.DistSpark
      } else {
        LUmode.LocalBreeze
      }
      case "breeze" => LUmode.LocalBreeze
      case "dist" => LUmode.DistSpark
      case _ => throw new IllegalArgumentException(s"Do not support mode $mode.")
    }
    val luResult: BlockMatrix = computeMode match {
      case LUmode.LocalBreeze =>
        val mat = toBreeze()
        val inverse = brzInv(mat)
        val blk = rows.context.parallelize(Seq((BlockID(0, 0), new SubMatrix(denseMatrix =inverse))), 1)
        new BlockMatrix(blk, inverse.rows, inverse.cols, 1, 1)
      case LUmode.DistSpark =>
        val subMatrixBaseSize = rows.context.getConf.getInt("marlin.inverse.basesize", 1000)
        val numBlksByRow, numBlksByCol = math.ceil(numRows().toDouble / subMatrixBaseSize.toDouble).toInt
        val subMatrixBase = math.ceil(numRows().toDouble / numBlksByRow.toDouble).toInt
        val pArray = Array.ofDim[Int](numRows().toInt)

        val partitioner = new HashPartitioner(2 * getClusterCores())
        var blkMat = this.toBlockMatrix(numBlksByRow, numBlksByCol).blocks //.partitionBy(partitioner)

        blkMat.cache()
        println("numBlkByRow is: " + numBlksByRow)
        println("original partitioner: " + partitioner.toString())
        val scatterRdds = Array.ofDim[RDD[(BlockID, SubMatrix)]](numBlksByRow - 1, 3)

        for (i <- 0 until numBlksByRow) {
          if (i == numBlksByRow - 1) {
            blkMat = blkMat.mapValues(block => new SubMatrix(denseMatrix = brzInv(block.denseBlock)))
          } else {
            val matFirst = blkMat.filter(block => block._1.row == i && block._1.column == i)
            val matSecond = blkMat.filter(block => block._1.row == i && block._1.column > i)
            val matThird = blkMat.filter(block => block._1.row > i && block._1.column == i)
            val matForth = blkMat.filter(block => block._1.column > i && block._1.row > i)

            val mat = matFirst.collect().head._2

            logInfo(s"Inverse iteration: $i")
            val t0 = System.currentTimeMillis()
            val inverse = brzInv(mat.denseBlock)
            logInfo(s"in iteration $i, in master inverse takes ${(System.currentTimeMillis() - t0) / 1000} seconds")
            val binv = rows.context.broadcast(inverse)

            scatterRdds(i)(0) = matFirst.mapValues(block => new SubMatrix(denseMatrix = binv.value)).cache()
            scatterRdds(i)(1) = matSecond.mapValues(block => {
              val t0 = System.currentTimeMillis()
              val tmp: BDM[Double] = -binv.value * block.denseBlock
              logInfo(s"in iteration $i, computation for A2 takes ${(System.currentTimeMillis() - t0) / 1000} seconds")
              new SubMatrix(denseMatrix = tmp)
            }).cache()
            scatterRdds(i)(2) = matThird.mapValues(block => {
              val t0 = System.currentTimeMillis()
              val tmp: BDM[Double] = -block.denseBlock * binv.value
              logInfo(s"in iteration $i, computation for A3 takes ${(System.currentTimeMillis() - t0) / 1000} seconds")
              new SubMatrix(denseMatrix = tmp)
            }).cache()

            val nSplitNum, mSplitNum = numBlksByRow - (i + 1)
            val kSplitNum = 1

            val matThirdEmit = matThird
              .mapPartitions({
                iter =>
                  iter.flatMap(t => {
                    val array = Array.ofDim[(BlockID, SubMatrix)](nSplitNum)
                    for (j <- 0 until nSplitNum) {
                      val seq = 0
                      array(j) = (BlockID(t._1.row, (j + i + 1), seq), t._2)
                    }
                    array
                  })
              }) //.partitionBy(partitioner)

            val matSecondEmit = matSecond
              .mapPartitions({
                iter =>
                  iter.flatMap{case(blkId, blk) =>
                    val array = Array.ofDim[(BlockID, SubMatrix)](mSplitNum)
                    for (j <- 0 until mSplitNum) {
                      val seq = 0
                      array(j) = (BlockID(j + i + 1, blkId.column, seq), blk)
                    }
                    array
                  }
              }) //.partitionBy(partitioner)
            println("matThirdEmit and matSecondEmit same partitioner? "
              + (matSecondEmit.partitioner == matThirdEmit.partitioner).toString)

            val mult = matThirdEmit.join(matSecondEmit, partitioner)
              .mapValues{case(blk1, blk2) =>
                (blk1.multiply(binv.value)).multiply(blk2)
              } //.partitionBy(partitioner)
            blkMat = matForth
              .join(mult, partitioner).mapValues(t => t._1.subtract(t._2)) //.partitionBy(partitioner).cache()
              .cache()
            //println(blkMat.toDebugString)
          }
        }

        for (i <- numBlksByRow - 2 to 0 by -1) {
          //val firstMat = scatterRdds(i)(0)
          val secondMat = scatterRdds(i)(1)
          val thirdMat = scatterRdds(i)(2)

          var m, k = numBlksByRow - 1 - i
          var n = 1

          val FourthEmit = blkMat.mapPartitions({
            iter =>
              iter.flatMap { case (blkId, blk) =>
                val array = Array.ofDim[(BlockID, SubMatrix)](n)
                for (j <- 0 until array.length) {
                  val seq = blkId.row * numBlksByRow * numBlksByCol + (i + j) * numBlksByCol + blkId.column
                  array(j) = (BlockID(blkId.row, i + j, seq), blk)
                }
                array
              }
          })

          val ThirdEmit = thirdMat.mapPartitions({
            iter =>
              iter.flatMap { case (blkId, blk) =>
                val array = Array.ofDim[(BlockID, SubMatrix)](m)
                for (j <- 0 until array.length) {
                  val seq = (i + 1 + j) * numBlksByRow * numBlksByCol + blkId.column * numBlksByCol + blkId.row
                  array(j) = (BlockID((i + j + 1), blkId.column, seq), blk)
                }
                array
              }
          })

          val multThird = FourthEmit.join(ThirdEmit)
            .map { case (blkId, (blk1, blk2)) =>
              val mat = blk1.multiply(blk2)
              (BlockID(blkId.row, blkId.column), mat)
            }.reduceByKey((a, b) => a.add(b)) //.partitionBy(partitioner)
          multThird.count()

          m = 1
          n = numBlksByRow - i - 1
          k = n
          val SecondEmit = secondMat.mapPartitions(iter =>
            iter.flatMap { case (blkId, blk) =>
              val array = Array.ofDim[(BlockID, SubMatrix)](n)
              for (j <- 0 until array.length) {
                val seq = blkId.row * k * n + (i + j + 1) * k + blkId.column
                array(j) = (BlockID(blkId.row, i + j + 1, seq), blk)
              }
              array
            })

          val FourthEmit2 = blkMat.mapPartitions(iter =>
            iter.flatMap { case (blkId, blk) =>
              val array = Array.ofDim[(BlockID, SubMatrix)](m)
              for (j <- 0 until array.length) {
                val seq = (i + j) * k * n + blkId.column * k + blkId.row
                array(j) = (BlockID(i + j, blkId.column, seq), blk)
              }
              array
            })

          val multSecond = SecondEmit.join(FourthEmit2)
            .map { case (blkId, (blk1, blk2)) =>
              val mat = (blk1.multiply(blk2))
              (BlockID(blkId.row, blkId.column), mat)
            }.reduceByKey((a, b) => a.add(b)) //.partitionBy(partitioner)

          val multFirst = scatterRdds(i)(1)
            .map { case(blkId, blk) => (BlockID(blkId.column, blkId.row), blk) }
            .join(multThird)
            .mapPartitions(iter =>
              iter.map { case(blkId, (blk1, blk2)) =>
                val mat = (blk1.multiply(blk2))
                (BlockID(i, i), mat)
              })
            .reduceByKey((a, b) => a.add(b))
            .join(scatterRdds(i)(0))
            .mapValues{case(m1, m2) => m1.add(m2)}
            .reduceByKey((a, b) => a.add(b))

          blkMat = blkMat.union(multSecond).union(multThird).union(multFirst)
            .partitionBy(partitioner).cache()
        }
        new BlockMatrix(blkMat, numRows(), numCols(), numBlksByRow, numBlksByCol)
    }
    luResult
  }

  /**
   * This matrix add another DistributedMatrix
   *
   * @param other another matrix in DenseVecMatrix format
   */
  final def add(other: DistributedMatrix): DenseVecMatrix = {
    other match {
      case that: DenseVecMatrix =>
        require(numRows() == that.numRows(), s"Dimension mismatch: ${numRows()} vs ${that.numRows()}")
        require(numCols() == that.numCols, s"Dimension mismatch: ${numCols()} vs ${that.numCols()}")

        val result = rows.join(that.rows).mapPartitions(iter => {
          iter.map{case(id, (v1, v2)) =>
            (id, ((v1 + v2).asInstanceOf[BDV[Double]]))}
        }, true)
        new DenseVecMatrix(result, numRows(), numCols())
      case that: BlockMatrix =>
        add(that.toDenseVecMatrix())
      case that: DistributedMatrix =>
        throw new IllegalArgumentException("Do not support this type " + that.getClass + "for add operation")
    }

  }

  /**
   * This matrix minus another DistributedMatrix
   *
   * @param other another matrix in DenseVecMatrix format
   */
  final def subtract(other: DistributedMatrix): DenseVecMatrix = {
    other match {
      case that: DenseVecMatrix =>
        require(numRows() == that.numRows(), s"Row dimension mismatch: ${numRows()} vs ${other.numRows()}")
        require(numCols == that.numCols, s"Column dimension mismatch: ${numCols()} vs ${other.numCols()}")

        val result = rows.join(that.rows).mapPartitions(iter => {
          iter.map(t =>
            (t._1, BDV(t._2._1.toArray.zip(t._2._2.toArray).map(x => x._1 - x._2))))
        }, true)
        new DenseVecMatrix(result, numRows(), numCols())
      case that: BlockMatrix =>
        subtract(that.toDenseVecMatrix())
    }

  }

  /**
   * Element in this matrix element-wise add another scalar
   *
   * @param b the number to be element-wise added
   */
  final def add(b: Double): DenseVecMatrix = {
    val result = rows.mapPartitions(iter => {
      iter.map(t => (t._1, BDV(t._2.toArray.map(_ + b))))
    }, true)
    new DenseVecMatrix(result, numRows(), numCols())
  }

  /**
   * Element in this matrix element-wise minus another scalar
   *
   * @param b a number to be element-wise subtracted
   */
  final def subtract(b: Double): DenseVecMatrix = {
    val result = rows.mapPartitions(iter => {
      iter.map(t => (t._1, BDV(t._2.toArray.map(_ - b))))
    }, true)
    new DenseVecMatrix(result, numRows(), numCols())
  }

  /**
   * Element in this matrix element-wise minus by another scalar
   *
   * @param b a number in the format of double
   */
  final def subtractBy(b: Double): DenseVecMatrix = {
    val result = rows.mapPartitions(iter => {
      iter.map(t => (t._1, BDV(t._2.toArray.map(b - _))))
    }, true)
    new DenseVecMatrix(result, numRows(), numCols())
  }

  /**
   * Element in this matrix element-wise multiply another scalar
   *
   * @param b a number in the format of double
   */
  final def multiply(b: Double): DenseVecMatrix = {
    val result = rows.mapPartitions(iter => {
      iter.map(t => (t._1, BDV(t._2.toArray.map(_ * b))))
    }, true)
    new DenseVecMatrix(result, numRows(), numCols())
  }

  /**
   * Element in this matrix element-wise divide another scalar
   *
   * @param b a number in the format of double
   * @return result in DenseVecMatrix type
   */
  final def divide(b: Double): DenseVecMatrix = {
    val result = rows.mapPartitions(iter => {
      iter.map(t => (t._1, BDV(t._2.toArray.map(_ / b))))
    }, true)
    new DenseVecMatrix(result, numRows(), numCols())
  }

  /**
   * Element in this matrix element-wise divided by another scalar
   *
   * @param b a number in the format of double
   */
  final def divideBy(b: Double): DenseVecMatrix = {
    val result = rows.mapPartitions(iter => {
      iter.map(t => (t._1, BDV(t._2.toArray.map(b / _))))
    }, preservesPartitioning = true)
    new DenseVecMatrix(result, numRows(), numCols())
  }

  /**
   * Sum all the elements in matrix ,note the Double.MaxValue is 1.7976931348623157E308
   *
   */
  def sum(): Double = {
    rows.mapPartitions(iter =>
      iter.map(t => t._2.toArray.sum), true).reduce(_ + _)
  }

  /**
   * count the elements of this DenseVecMatrix
   */
  def elementsCount(): Long = {
    rows.count()
  }

  /**
   * Matrix-matrix dot product, the two input matrices must have the same row and column dimension
   * @param other the matrix to be dot product
   */
  def dotProduct(other: DistributedMatrix): DistributedMatrix = {
    require(numRows() == other.numRows(), s"row dimension mismatch ${numRows()} vs ${other.numRows()}")
    require(numCols() == other.numCols(), s"column dimension mismatch ${numCols()} vs ${other.numCols()}")
    other match {
      case that: DenseVecMatrix =>
        val result = rows.join(that.rows).mapPartitions(iter => {
          iter.map(t => {
            val array = t._2._1.toArray.zip(t._2._2.toArray).map(x => x._1 * x._2)
            (t._1, BDV(array))
          })
        }, true)
        new DenseVecMatrix(result, numRows(), numCols())
      case that: BlockMatrix =>
        dotProduct(that.toDenseVecMatrix())
    }
  }

  /**
   * Get sub matrix according to the given range of rows
   *
   * @param startRow the start row of the subMatrix, this row is included
   * @param endRow the end row of the subMatrix, this row is included
   */
  final def sliceByRow(startRow: Long, endRow: Long): DenseVecMatrix = {
    require((startRow >= 0 && endRow <= numRows() && startRow <= endRow),
      s"start row or end row mismatch the matrix num of rows")

    new DenseVecMatrix(rows.filter(t => (t._1 >= startRow && t._1 <= endRow)).map(t => (t._1 - startRow, t._2)), endRow - startRow + 1, numCols())
  }

  /**
   * get sub matrix according to the given range of column
   *
   * @param startCol the start column of the subMatrix, this column is included
   * @param endCol the end column of the subMatrix, this column is included
   */
  final def sliceByColumn(startCol: Int, endCol: Int): DenseVecMatrix = {
    require((startCol >= 0 && endCol <= numCols() && startCol <= endCol),
      s"start column or end column mismatch the matrix num of columns")

    new DenseVecMatrix(rows.map(t => (t._1, BDV(t._2.toArray.slice(startCol, endCol + 1)))), numRows(), endCol - startCol + 1)
  }

  /**
   * get sub matrix according to the given range of column
   *
   * @param startRow the start row of the subMatrix, this row is included
   * @param endRow the end row of the subMatrix, this row is included
   * @param startCol the start column of the subMatrix, this column is included
   * @param endCol the end column of the subMatrix, this column is included
   */
  final def getSubMatrix(startRow: Long, endRow: Long, startCol: Int, endCol: Int): DenseVecMatrix = {
    require((startRow >= 0 && endRow <= numRows()), s"start row or end row dismatch the matrix num of rows")
    require((startCol >= 0 && endCol <= numCols()),
      s"start column or end column dismatch the matrix num of columns")

    new DenseVecMatrix(rows
      .filter(t => (t._1 >= startRow && t._1 <= endRow))
      .map(t => (t._1 - startRow, BDV(t._2.toArray.slice(startCol, endCol + 1)))), endRow - startRow + 1, endCol - startCol + 1)
  }

  /**
   * compute the norm of this matrix, work in progress
   *
   * @param mode the same with Matlab operations,
   *             `1` means get the 1-norm, the largest column sum of matrix
   *             `2` means get the largest singular value, the default mode --- need to do
   *             `inf` means the infinity norm, the largest row sum of matrix
   *             `fro` means the Frobenius-norm of matrix -- need to do
   */
  def norm(mode: String): Double = {
    mode match {
      case "1" => {
        rows.mapPartitions(iter => {
          val columnAcc = new ParArray[Double](numCols().toInt)
          iter.map(t => {
            val arr = t._2.toArray
            for (i <- 0 until arr.length) {
              columnAcc(i) += math.abs(arr(i))
            }
          })
          columnAcc.toIterator
        }).max()
      }
      //      case "2" => {
      //
      //      }
      case "inf" => {
        rows.map(t => t._2.toArray.reduce(_ + _)).max()
      }
      //      case "for" => {
      //
      //      }
    }
  }

  /**
   * use stochastic gradient descent (SGD) to obtain the sum of gradient
   * each row of matrix should be the form of (label, features)
   */
  def lr(stepSize: Double, iters: Int): Array[Double] = {
    val featureSize = numCols().toInt
    var weights = BDV(Array.fill(featureSize)(0.0))
    val dataSize = numRows()

    val data = rows.mapPartitions(rowParts => {
      rowParts.map(row => {
        val arr = row._2.toArray
        val label = arr(0)
        arr(0) = 1 // the first element 1 is the intercept
        (label, BDV(arr))
      })
    }).cache()
    for (i <- 1 to iters) {
      val gradientRDD = data.mapPartitions(part => {
        part.map(x => {
          val label = x._1
          val features = x._2
          val margin = -1.0 * features.dot(weights)
          val gradientMul = (1.0 / (1.0 + math.exp(margin))) - label
          val gradient: BDV[Double] = features * gradientMul
          gradient
        })
      })
      val delta = gradientRDD.reduce((a, b) => {
        a + b
      })
      weights = weights - delta * (stepSize / dataSize / math.sqrt(i))
    }
    weights.toArray
  }

  /**
   * Save the result to the HDFS or local file system
   *
   * @param path the path to store the DenseVecMatrix in HDFS or local file system
   */
  def saveToFileSystem(path: String) {

    rows.map(t => (NullWritable.get(), new Text(t._1 + ":" + t._2.toString)))
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
  }

  /**
   * Save the result to HDFS with a description file name `_description`, the description file content is like:
   * MatrixName[TAB]name (if not available, use N/A)
   * MatrixSize[TAB]row column
   *
   * @param path
   */
  def saveWithDescription(path: String): Unit = {
    rows.map(t => (NullWritable.get(), new Text(t._1 + ":" + t._2.toString)))
      .saveAsHadoopFile[TextOutputFormat[NullWritable, Text]](path)
    val conf = new Configuration()
    val hdfs = FileSystem.get(conf)
    val out = hdfs.create(new Path(path + "/_description"))
    val info = "MatrixName\tN/A\nMatrixSize\t" + numRows() + " " + numCols()
    out.write(info.getBytes())
    out.close()
  }

  //  /**
  //   * A simple wraper of saving the DenseVecMatrix to SequenceFile
  //   *
  //   * @param path matrix file path
  //   */
  //  def saveSequenceFile(path: String) {
  //    rows.saveAsSequenceFile(path)
  //  }

  /**
   * transform the DenseVecMatrix to several blocks
   *
   * @param m num of subMatrix along the row side set by user,
   *          the actual num of subMatrix along the row side is `blksByRow`
   * @param k num of subMatrix along the column side set by user,
   *          the actual num of subMatrix along the col side is `blksByCol`
   *
   */
  def toBlocks(m: Int, k: Int, n: Int, mode: String): RDD[(BlockID, BDM[Double])] = {
    require(mode.toLowerCase.equals("right") || mode.toLowerCase.equals("left"),
      s"only 'right' mode or 'left' mode is supported, you should change mode $mode")
    require(m > 0 && k > 0 && n > 0, s"not supported (m, k, n): ($m, $k, $n)")
    val mRows = numRows().toInt
    val mColumns = numCols().toInt
    if (mode.toLowerCase.equals("right")) {
      val mBlockRowSize = math.ceil(mRows.toDouble / m.toDouble).toInt
      val mBlockColSize = math.ceil(mColumns.toDouble / k.toDouble).toInt
      val blksByRow = math.ceil(mRows.toDouble / mBlockRowSize).toInt
      val blksByCol = math.ceil(mColumns.toDouble / mBlockColSize).toInt

      if (blksByCol == 1) {
        rows.mapPartitions(iter => {
          iter.map(t => {
            (t._1.toInt / mBlockRowSize, t)
          })
        }).groupByKey().mapPartitions(iter =>
          iter.flatMap { case (blockRow, iteratable) =>
            val rowLen = if ((blockRow + 1) * mBlockRowSize > numRows()) {
              numRows().toInt - blockRow * mBlockRowSize
            } else {
              mBlockRowSize
            }
            val mat = BDM.zeros[Double](rowLen, mBlockColSize)
            val iterator = iteratable.iterator
            while (iterator.hasNext) {
              val (index, vec) = iterator.next()
              mat(index.toInt - blockRow * mBlockRowSize, ::) := vec.t
            }
            Iterator.tabulate[(BlockID, BDM[Double])](n)(i => {
              val seq = blockRow * n * k + i * k
              (BlockID(blockRow, i, seq), mat)
            })
          })
      } else {
        rows.mapPartitions(iter => {
          iter.flatMap { case (index, vector) =>
            var startColumn = 0
            var endColumn = 0
            Iterator.tabulate[(BlockID, (Long, BDV[Double]))](blksByCol)(i => {
              startColumn = i * mBlockColSize
              endColumn = math.min(startColumn + mBlockColSize, mColumns)
              (BlockID(index.toInt / mBlockRowSize, i), (index, vector.slice(startColumn, endColumn).copy))
            })
          }
        }).groupByKey().mapPartitions(iter =>
          iter.flatMap { case (blkId, iterable) =>
            val colBase = blkId.column * mBlockColSize
            val rowBase = blkId.row * mBlockRowSize
            //the block's size: rows & columns
            var smRows = mBlockRowSize
            if ((rowBase + mBlockRowSize - 1) >= mRows) {
              smRows = mRows - rowBase
            }
            var smCols = mBlockColSize
            if ((colBase + mBlockColSize - 1) >= mColumns) {
              smCols = mColumns - colBase
            }
            //to generate the local matrix, be careful, the array is column major
            val mat = BDM.zeros[Double](smRows, smCols)
            val iterator = iterable.iterator
            while (iterator.hasNext) {
              val (index, vec) = iterator.next()
              mat((index - rowBase).toInt, ::) := vec.t
            }
            Iterator.tabulate[(BlockID, BDM[Double])](n)(i => {
              val seq = blkId.row * n * k + i * k + blkId.column
              (BlockID(blkId.row, i, seq), mat)
            })
          })
      }
    } else {
      // if the mode is left, which means transform the left row matirx to several blocks
      val mBlockRowSize = math.ceil(mRows.toDouble / k.toDouble).toInt
      val mBlockColSize = math.ceil(mColumns.toDouble / n.toDouble).toInt
      val blksByRow = math.ceil(mRows.toDouble / mBlockRowSize).toInt
      val blksByCol = math.ceil(mColumns.toDouble / mBlockColSize).toInt

      if (blksByCol == 1) {
        rows.mapPartitions(iter => {
          iter.map(t => {
            (t._1.toInt / mBlockRowSize, t)
          })
        }).groupByKey().mapPartitions(iter =>
          iter.flatMap { case (blockRow, iteratable) =>
            val rowLen = if ((blockRow + 1) * mBlockRowSize > numRows()) {
              numRows().toInt - blockRow * mBlockRowSize
            } else {
              mBlockRowSize
            }
            val mat = BDM.zeros[Double](rowLen, mBlockColSize)
            val iterator = iteratable.iterator
            while (iterator.hasNext) {
              val (index, vec) = iterator.next()
              mat(index.toInt - blockRow * mBlockRowSize, ::) := vec.t
            }
            Iterator.tabulate[(BlockID, BDM[Double])](m)(i => {
              val seq = i * n * k + blockRow
              (BlockID(i, 0, seq), mat)
            })
          })
      } else {
        rows.mapPartitions(iter => {
          iter.flatMap { case (index, vector) =>
            var startColumn = 0
            var endColumn = 0
            Iterator.tabulate[(BlockID, (Long, BDV[Double]))](blksByCol)(i => {
              startColumn = i * mBlockColSize
              endColumn = math.min(startColumn + mBlockColSize, mColumns)
              (BlockID(index.toInt / mBlockRowSize, i), (index, vector.slice(startColumn, endColumn).copy))
            })
          }
        }).groupByKey().mapPartitions(iter =>
          iter.flatMap { case (blkId, iterable) =>
            val colBase = blkId.column * mBlockColSize
            val rowBase = blkId.row * mBlockRowSize
            //the block's size: rows & columns
            var smRows = mBlockRowSize
            if ((rowBase + mBlockRowSize - 1) >= mRows) {
              smRows = mRows - rowBase
            }
            var smCols = mBlockColSize
            if ((colBase + mBlockColSize - 1) >= mColumns) {
              smCols = mColumns - colBase
            }
            val mat = BDM.zeros[Double](smRows, smCols)
            val iterator = iterable.iterator
            while (iterator.hasNext) {
              val (index, vec) = iterator.next()
              mat((index - rowBase).toInt, ::) := vec.t
            }
            Iterator.tabulate[(BlockID, BDM[Double])](m)(i => {
              val seq = i * n * k + blkId.column * k + blkId.row
              (BlockID(i, blkId.column, seq), mat)
            })
          })
      }
    }
  }


  def toBlockMatrix(splitStatusByRow: Array[ArrayBuffer[(Int, (Int, Int), (Int, Int))]],
                    blkNumByRow: Int): BlockMatrix = {
    val mostBlkRowLen = math.ceil(numRows().toDouble / blkNumByRow.toDouble).toInt
    val blksByRow = math.ceil(numRows().toDouble / mostBlkRowLen).toInt
    val blocks = rows.mapPartitionsWithIndex { (id, iter) =>
      val array = Array.ofDim[(BlockID, (Int, Int, BDM[Double]))](splitStatusByRow(id).size)
      var count = 0
      for ((rowId, (oldRow1, oldRow2), (newRow1, newRow2)) <- splitStatusByRow(id)) {
        val rowBlock = oldRow2 - oldRow1 + 1
        val blk = BDM.zeros[Double](rowBlock, numCols().toInt)
        for (i <- 0 until rowBlock) {
          blk(i, ::) := iter.next()._2.t
        }
        array(count) = (BlockID(rowId, 0), (newRow1, newRow2, blk))
        count += 1
      }
      array.toIterator
    }.groupByKey().mapPartitions { iter =>
      iter.map { case (blkId, iterable) =>
        val rowLen = if ((blkId.row + 1) * mostBlkRowLen > numRows()) {
          (numRows() - blkId.row * mostBlkRowLen).toInt
        } else mostBlkRowLen
        val mat = BDM.zeros[Double](rowLen, numCols().toInt)
        val iterator = iterable.iterator
        for ((rowStart, rowEnd, blk) <- iterator) {
          mat(rowStart to rowEnd, ::) := blk
        }
        (blkId, new SubMatrix(denseMatrix = mat))
      }
    }
    new BlockMatrix(blocks, numRows(), numCols(), blksByRow, 1)
  }

  def toBlockMatrix(numByRow: Int, numByCol: Int): BlockMatrix = {
    val mRows = numRows().toInt
    val mColumns = numCols().toInt
    val mBlockRowSize = math.ceil(mRows.toDouble / numByRow.toDouble).toInt
    val mBlockColSize = math.ceil(mColumns.toDouble / numByCol.toDouble).toInt
    val blksByRow = math.ceil(mRows.toDouble / mBlockRowSize).toInt
    val blksByCol = math.ceil(mColumns.toDouble / mBlockColSize).toInt

    if (blksByCol == 1) {
      val result = rows.mapPartitions(iter => {
        iter.map(t => {
          (t._1.toInt / mBlockRowSize, t)
        })
      }).groupByKey().mapPartitions(iter => {
        iter.map { case (blockRow, iteratable) =>
          val rowLen = if ((blockRow + 1) * mBlockRowSize > numRows()) {
            numRows().toInt - blockRow * mBlockRowSize
          } else mBlockRowSize
          val mat = BDM.zeros[Double](rowLen, mBlockColSize)
          val iterator = iteratable.iterator
          while (iterator.hasNext) {
            val (index, vec) = iterator.next()
            mat(index.toInt - blockRow * mBlockRowSize, ::) := vec.t
          }
          (BlockID(blockRow, 0), new SubMatrix(denseMatrix = mat))
        }
      })
      new BlockMatrix(result, numRows(), numCols(), blksByRow, blksByCol)
    } else {
      val result = rows.mapPartitions(iter => {
        iter.flatMap { case (index, vector) =>
          var startColumn = 0
          var endColumn = 0
          var arrayBuf = new ArrayBuffer[(BlockID, (Long, BDV[Double]))]
          var i = 0
          while (endColumn < mColumns) {
            startColumn = i * mBlockColSize
            endColumn = startColumn + mBlockColSize
            if (endColumn >= mColumns) {
              endColumn = mColumns
            }
            arrayBuf += ((BlockID(index.toInt / mBlockRowSize, i), (index, vector.slice(startColumn, endColumn).copy)))
            i += 1
          }
          arrayBuf
        }
      }).groupByKey()
        .mapPartitions(iter =>
          iter.map { case (blkId, iterable) =>
            val colBase = blkId.column * mBlockColSize
            val rowBase = blkId.row * mBlockRowSize
            var smRows = mBlockRowSize
            if ((rowBase + mBlockRowSize - 1) >= mRows) {
              smRows = mRows - rowBase
            }
            var smCols = mBlockColSize
            if ((colBase + mBlockColSize - 1) >= mColumns) {
              smCols = mColumns - colBase
            }
            val mat = BDM.zeros[Double](smRows, smCols)
            val iterator = iterable.iterator
            while (iterator.hasNext) {
              val (index, vector) = iterator.next()
              mat((index - rowBase).toInt, ::) := vector.t
            }
            (blkId, new SubMatrix(denseMatrix = mat))
          }, true)
      new BlockMatrix(result, numRows(), numCols(), blksByRow, blksByCol)
    }
  }

  /**
   * transform the DenseVecMatrix to SparseVecMatrix
   */
  def toSparseVecMatrix(): SparseVecMatrix = {
    val result = rows.mapPartitions(iter => {
      iter.map(t => {
        val array = t._2.toArray
        val indices = new ArrayBuffer[Int]()
        val values = new ArrayBuffer[Double]()
        for (i <- 0 until array.length) {
          if (array(i) != 0) {
            indices += i
            values += array(i)
          }
        }
        if (indices.size >= 0) {
          (t._1, new BSV[Double](indices.toArray, values.toArray, indices.size))
        } else {
          throw new IllegalArgumentException("indices size is empty")
        }
      })
    })
    new SparseVecMatrix(result, numRows(), numCols())
  }

  def toBlockMatrixFromCoordinate(blksByRow: Int, blksByCol: Int): BlockMatrix = {
    require(blksByCol > 0 && blksByRow > 0, s"blksByRow and blksByCol should be larger than 0")
    val rowsPerBlock = math.ceil(numRows().toDouble / blksByRow.toDouble).toInt
    val colsPerBlock = math.ceil(numCols().toDouble / blksByCol.toDouble).toInt
    val newBlksByRow = math.ceil(numRows().toDouble / rowsPerBlock.toDouble).toInt
    val newBlksByCol = math.ceil(numCols().toDouble / colsPerBlock.toDouble).toInt
    val blocks = rows.flatMap { case (index, values) =>
      Iterator.tabulate(values.size)(i => (index, i, values(i)))
    }.map { case (rowIndex, colIndex, value) =>
      val blkRowIndex = (rowIndex / rowsPerBlock).toInt
      val blkColIndex = colIndex / colsPerBlock
      val rowId = rowIndex % rowsPerBlock
      val colId = colIndex % colsPerBlock
      (BlockID(blkRowIndex, blkColIndex), (rowId, colId, value))
    }.groupByKey().map { case (blkId, entry) =>
      val smRows = math.min(numRows() - blkId.row * rowsPerBlock, rowsPerBlock).toInt
      val smCols = math.min(numCols() - blkId.column * colsPerBlock, colsPerBlock).toInt
      val matrix = BDM.zeros[Double](smRows, smCols)
      for ((i, j, v) <- entry) {
        matrix(i.toInt, j) = v
      }
      (blkId, new SubMatrix(denseMatrix =matrix))
    }
    new BlockMatrix(blocks, numRows(), numCols(), newBlksByRow, newBlksByCol)
  }

  def toDataFrame(sqlContext: SQLContext,
                  schemaStringArray: Array[String],
                  rowNumWrite: Boolean = true): DataFrame = {
    val sch = schemaStringArray.map(fieldName => StructField(fieldName, DoubleType, true))
    val schema = if (rowNumWrite) {
      StructType(StructField("__rowNum", LongType, true) +: sch)
    } else {
      StructType(sch)
    }
    val rowsRDD = if (rowNumWrite) {
      rows.map { case (ind, row) => Row((ind +: row.toArray): _ *) }
    } else {
      rows.map { case (ind, row) => Row((row.toArray): _*) }
    }
    sqlContext.createDataFrame(rowsRDD, schema)
  }

  /**
   * Print the matrix out
   */
  def print() {
    if (numRows() > 20) {
      rows.take(20).foreach(t => println("index: " + t._1 + ", vector: " + t._2.slice(0, math.min(8, t._2.length))))
      println("there are " + numRows() + " rows total...")
    } else {
      rows.collect().foreach(t => println("index: " + t._1 + ", vector: " + t._2.slice(0, math.min(8, t._2.length))))
    }
  }

  /**
   * Print the whole matrix out
   */
  def printAll() {
    rows.collect().foreach(t => println("index: " + t._1 + ", vector: " + t._2.data.mkString(",")))
  }

  /**
   * A transpose view of this matrix
   */
  def transpose(): BlockMatrix = {
    require(numRows() < Int.MaxValue, s"the row length of matrix is too large to transpose")
    val sc = rows.context
    val blkByRow = if (!sc.getConf.getOption("spark.default.parallelism").isEmpty) {
      sc.getConf.get("spark.default.parallelism").toInt
    } else {
      sc.defaultMinPartitions
    }
    toBlockMatrix(math.min(blkByRow, numRows().toInt / 2), 1).transpose()
  }

  /**
   * A transpose view of this matrix
   */
  def transpose(numBlocks: Int): BlockMatrix = {
    toBlockMatrix(math.min(numBlocks, numRows().toInt / 2), 1).transpose()
  }

  /**
   * Multiplies the Gramian matrix `A^T A` by a dense vector on the right without computing `A^T A`.
   *
   * @param v a dense vector whose length must match the number of columns of this matrix
   * @return a dense vector representing the product
   */
  private[marlin] def multiplyGramianMatrixBy(v: BDV[Double]): BDV[Double] = {
    val n = numCols().toInt
    val vbr = rows.context.broadcast(v)
    rows.aggregate(BDV.zeros[Double](n))(
      seqOp = (U, r) => {
        val rBrz = r._2
        val a = rBrz.dot(vbr.value)
        rBrz match {
          // use specialized axpy for better performance
          case _: BDV[_] => brzAxpy(a, rBrz.asInstanceOf[BDV[Double]], U)
          case _ => throw new UnsupportedOperationException(
            s"Do not support vector operation from type ${rBrz.getClass.getName}.")
        }
        U
      }, combOp = (U1, U2) => U1 += U2)
  }

  /**
   * Computes the Gramian matrix `A^T A`.
   */
  private[marlin] def computeGramianMatrix(): Matrix = {
    def checkNumColumns(cols: Int): Unit = {
      if (cols > 65535) {
        throw new IllegalArgumentException(s"Argument with more than 65535 cols: $cols")
      }
      if (cols > 10000) {
        val mem = cols * cols * 8
        logWarning(s"$cols columns will require at least $mem bytes of memory!")
      }
    }
    val n = numCols().toInt
    checkNumColumns(n)
    // Computes n*(n+1)/2, avoiding overflow in the multiplication.
    // This succeeds when n <= 65535, which is checked above
    val nt: Int = if (n % 2 == 0) ((n / 2) * (n + 1)) else (n * ((n + 1) / 2))
    // Compute the upper triangular part of the gram matrix.
    val GU = rows.aggregate(new BDV[Double](new Array[Double](nt)))(
      seqOp = (U, v) => {
        DenseVecMatrix.dspr(1.0, v._2, U.data)
        U
      }, combOp = (U1, U2) => U1 += U2)
    DenseVecMatrix.triuToFull(n, GU.data)
  }

  /**
   * Computes singular value decomposition of this matrix. Denote this matrix by A (m x n). This
   * will compute matrices U, S, V such that A ~= U * S * V', where S contains the leading k
   * singular values, U and V contain the corresponding singular vectors.
   *
   * At most k largest non-zero singular values and associated vectors are returned. If there are k
   * such values, then the dimensions of the return will be:
   * - U is a RowMatrix of size m x k that satisfies U' * U = eye(k),
   * - s is a Vector of size k, holding the singular values in descending order,
   * - V is a Matrix of size n x k that satisfies V' * V = eye(k).
   *
   * We assume n is smaller than m. The singular values and the right singular vectors are derived
   * from the eigenvalues and the eigenvectors of the Gramian matrix A' * A. U, the matrix
   * storing the right singular vectors, is computed via matrix multiplication as
   * U = A * (V * S^-1^), if requested by user. The actual method to use is determined
   * automatically based on the cost:
   * - If n is small (n &lt; 100) or k is large compared with n (k > n / 2), we compute the Gramian
   * matrix first and then compute its top eigenvalues and eigenvectors locally on the driver.
   * This requires a single pass with O(n^2^) storage on each executor and on the driver, and
   * O(n^2^ k) time on the driver.
   * - Otherwise, we compute (A' * A) * v in a distributive way and send it to ARPACK's DSAUPD to
   * compute (A' * A)'s top eigenvalues and eigenvectors on the driver node. This requires O(k)
   * passes, O(n) storage on each executor, and O(n k) storage on the driver.
   *
   * Several internal parameters are set to default values. The reciprocal condition number rCond
   * is set to 1e-9. All singular values smaller than rCond * sigma(0) are treated as zeros, where
   * sigma(0) is the largest singular value. The maximum number of Arnoldi update iterations for
   * ARPACK is set to 300 or k * 3, whichever is larger. The numerical tolerance for ARPACK's
   * eigen-decomposition is set to 1e-10.
   *
   * @note The conditions that decide which method to use internally and the default parameters are
   *       subject to change.
   *
   * @param k number of leading singular values to keep (0 &lt; k &lt;= n).
   *          It might return less than k if
   *          there are numerically zero singular values or there are not enough Ritz values
   *          converged before the maximum number of Arnoldi update iterations is reached (in case
   *          that matrix A is ill-conditioned).
   * @param computeU whether to compute U
   * @param rCond the reciprocal condition number. All singular values smaller than rCond * sigma(0)
   *              are treated as zero, where sigma(0) is the largest singular value.
   * @return SingularValueDecomposition(U, s, V). U = null if computeU = false.
   */
  def computeSVD(
                  k: Int,
                  computeU: Boolean = false,
                  rCond: Double = 1e-9): (DenseVecMatrix, BDV[Double], Matrix) = {
    // maximum number of Arnoldi update iterations for invoking ARPACK
    val maxIter = math.max(300, k * 3)
    // numerical tolerance for invoking ARPACK
    val tol = 1e-10

    computeSVD(k, computeU, rCond, maxIter, tol, "auto")
  }

  /**
   * The actual SVD implementation, visible for testing.
   *
   * @param k number of leading singular values to keep (0 &lt; k &lt;= n)
   * @param computeU whether to compute U
   * @param rCond the reciprocal condition number
   * @param maxIter max number of iterations (if ARPACK is used)
   * @param tol termination tolerance (if ARPACK is used)
   * @param mode computation mode (auto: determine automatically which mode to use,
   *             local-svd: compute gram matrix and computes its full SVD locally,
   *             local-eigs: compute gram matrix and computes its top eigenvalues locally,
   *             dist-eigs: compute the top eigenvalues of the gram matrix distributively)
   * @return SingularValueDecomposition(U, s, V). U = null if computeU = false.
   */
  private[marlin] def computeSVD(
                                  k: Int,
                                  computeU: Boolean,
                                  rCond: Double,
                                  maxIter: Int,
                                  tol: Double,
                                  mode: String): (DenseVecMatrix, BDV[Double], Matrix) = {
    val n = numCols().toInt
    require(k > 0 && k <= n, s"Request up to n singular values but got k=$k and n=$n.")
    object SVDMode extends Enumeration {
      val LocalARPACK, LocalLAPACK, DistARPACK = Value
    }
    val computeMode = mode match {
      case "auto" =>
        // TODO: The conditions below are not fully tested.
        if (n < 100 || k > n / 2) {
          // If n is small or k is large compared with n, we better compute the Gramian matrix first
          // and then compute its eigenvalues locally, instead of making multiple passes.
          if (k < n / 3) {
            SVDMode.LocalARPACK
          } else {
            SVDMode.LocalLAPACK
          }
        } else {
          // If k is small compared with n, we use ARPACK with distributed multiplication.
          SVDMode.DistARPACK
        }
      case "local-svd" => SVDMode.LocalLAPACK
      case "local-eigs" => SVDMode.LocalARPACK
      case "dist-eigs" => SVDMode.DistARPACK
      case _ => throw new IllegalArgumentException(s"Do not support mode $mode.")
    }
    // Compute the eigen-decomposition of A' * A.
    val (sigmaSquares: BDV[Double], u: BDM[Double]) = computeMode match {
      case SVDMode.LocalARPACK =>
        require(k < n, s"k must be smaller than n in local-eigs mode but got k=$k and n=$n.")
        val G = computeGramianMatrix().toBreeze.asInstanceOf[BDM[Double]]
        EigenValueDecomposition.symmetricEigs(v => G * v, n, k, tol, maxIter)
      case SVDMode.LocalLAPACK =>
        val G = computeGramianMatrix().toBreeze.asInstanceOf[BDM[Double]]
        val svdResult = brzSvd(G)
        (svdResult.S, svdResult.U)
      case SVDMode.DistARPACK =>
        if (rows.getStorageLevel == StorageLevel.NONE) {
          logWarning("The input data is not directly cached, which may hurt performance if its"
            + " parent RDDs are also uncached.")
        }
        require(k < n, s"k must be smaller than n in dist-eigs mode but got k=$k and n=$n.")
        EigenValueDecomposition.symmetricEigs(multiplyGramianMatrixBy, n, k, tol, maxIter)
    }
    val sigmas: BDV[Double] = brzSqrt(sigmaSquares)
    // Determine the effective rank.
    val sigma0 = sigmas(0)
    val threshold = rCond * sigma0
    var i = 0
    // sigmas might have a length smaller than k, if some Ritz values do not satisfy the convergence
    // criterion specified by tol after max number of iterations.
    // Thus use i < min(k, sigmas.length) instead of i < k.
    if (sigmas.length < k) {
      logWarning(s"Requested $k singular values but only found ${sigmas.length} converged.")
    }
    while (i < math.min(k, sigmas.length) && sigmas(i) >= threshold) {
      i += 1
    }
    val sk = i
    if (sk < k) {
      logWarning(s"Requested $k singular values but only found $sk nonzeros.")
    }
    // Warn at the end of the run as well, for increased visibility.
    if (computeMode == SVDMode.DistARPACK && rows.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data was not directly cached, which may hurt performance if its"
        + " parent RDDs are also uncached.")
    }
    val s = BDV(Arrays.copyOfRange(sigmas.data, 0, sk))

    val V = Matrices.dense(n, sk, Arrays.copyOfRange(u.data, 0, n * sk)).asInstanceOf[DenseMatrix]
    if (computeU) {
      // N = Vk * Sk^{-1}
      val N = new BDM[Double](n, sk, Arrays.copyOfRange(u.data, 0, n * sk))
      var i = 0
      var j = 0
      while (j < sk) {
        i = 0
        val sigma = sigmas(j)
        while (i < n) {
          N(i, j) /= sigma
          i += 1
        }
        j += 1
      }
      val U = this.multiply(N)
      (U, s, V)
    } else {
      (null, s, V)
    }
  }


  /**
   * In each partition, first make all the row elements into an DenseMatrix, after multiplication break down the
   * matrix to several rows.
   * @param B another local matrix to be multiplied
   */
  def multiply(B: BDM[Double]): DenseVecMatrix = {
    require(numCols() == B.rows, s"Dimension mismatch during matrix-matrix multiplication: ${numCols()} vs ${B.rows}")
    val mat = rows.sparkContext.broadcast(B.t.copy)
    val result = rows.mapPartitions { iter =>
      val arrayBuffer = new ArrayBuffer[(Long, BDV[Double])]()
      while (iter.hasNext) {
        arrayBuffer += iter.next()
      }
      val rowVecLen = arrayBuffer.size
      val colVecLen = numCols().toInt
      val rowsMat = BDM.zeros[Double](colVecLen, rowVecLen)
      val idArray = Array.ofDim[Long](rowVecLen)
      for (i <- 0 until rowVecLen) {
        idArray(i) = arrayBuffer(i)._1
        rowsMat(::, i) := arrayBuffer(i)._2
      }
      val matrix: BDM[Double] = mat.value * rowsMat
      Iterator.tabulate[(Long, BDV[Double])](rowVecLen)(i => (idArray(i), matrix(::, i)))
    }
    new DenseVecMatrix(result, 0L, B.cols)
  }

}


object DenseVecMatrix {
  /**
   * Adds alpha * x * x.t to a matrix in-place. This is the same as BLAS's DSPR.
   *
   * @param U the upper triangular part of the matrix packed in an array (column major)
   */
  private def dspr(alpha: Double, v: BDV[Double], U: Array[Double]): Unit = {
    val n = v.size
    v match {
      case dv: BDV[Double] =>
        blas.dspr("U", n, alpha, dv.data, 1, U)
    }
  }

  /**
   * Fills a full square matrix from its upper triangular part.
   */
  private def triuToFull(n: Int, U: Array[Double]): Matrix = {
    val G = new BDM[Double](n, n)
    var row = 0
    var col = 0
    var idx = 0
    var value = 0.0
    while (col < n) {
      row = 0
      while (row < col) {
        value = U(idx)
        G(row, col) = value
        G(col, row) = value
        idx += 1
        row += 1
      }
      G(col, col) = U(idx)
      idx += 1
      col += 1
    }
    Matrices.dense(n, n, G.data)
  }
}

private[marlin] object EigenValueDecomposition {
  /**
   * Compute the leading k eigenvalues and eigenvectors on a symmetric square matrix using ARPACK.
   * The caller needs to ensure that the input matrix is real symmetric. This function requires
   * memory for `n*(4*k+4)` doubles.
   *
   * @param mul a function that multiplies the symmetric matrix with a DenseVector.
   * @param n dimension of the square matrix (maximum Int.MaxValue).
   * @param k number of leading eigenvalues required, 0 < k < n.
   * @param tol tolerance of the eigs computation.
   * @param maxIterations the maximum number of Arnoldi update iterations.
   * @return a dense vector of eigenvalues in descending order and a dense matrix of eigenvectors
   *         (columns of the matrix).
   * @note The number of computed eigenvalues might be smaller than k when some Ritz values do not
   *       satisfy the convergence criterion specified by tol (see ARPACK Users Guide, Chapter 4.6
   *       for more details). The maximum number of Arnoldi update iterations is set to 300 in this
   *       function.
   */
  private[marlin] def symmetricEigs(
                                     mul: BDV[Double] => BDV[Double],
                                     n: Int,
                                     k: Int,
                                     tol: Double,
                                     maxIterations: Int): (BDV[Double], BDM[Double]) = {
    // TODO: remove this function and use eigs in breeze when switching breeze version
    require(n > k, s"Number of required eigenvalues $k must be smaller than matrix dimension $n")
    val arpack = ARPACK.getInstance()
    // tolerance used in stopping criterion
    val tolW = new doubleW(tol)
    // number of desired eigenvalues, 0 < nev < n
    val nev = new intW(k)
    // nev Lanczos vectors are generated in the first iteration
    // ncv-nev Lanczos vectors are generated in each subsequent iteration
    // ncv must be smaller than n
    val ncv = math.min(2 * k, n)
    // "I" for standard eigenvalue problem, "G" for generalized eigenvalue problem
    val bmat = "I"
    // "LM" : compute the NEV largest (in magnitude) eigenvalues
    val which = "LM"
    var iparam = new Array[Int](11)
    // use exact shift in each iteration
    iparam(0) = 1
    // maximum number of Arnoldi update iterations, or the actual number of iterations on output
    iparam(2) = maxIterations
    // Mode 1: A*x = lambda*x, A symmetric
    iparam(6) = 1
    var ido = new intW(0)
    var info = new intW(0)
    var resid = new Array[Double](n)
    var v = new Array[Double](n * ncv)
    var workd = new Array[Double](n * 3)
    var workl = new Array[Double](ncv * (ncv + 8))
    var ipntr = new Array[Int](11)
    // call ARPACK's reverse communication, first iteration with ido = 0
    arpack.dsaupd(ido, bmat, n, which, nev.`val`, tolW, resid, ncv, v, n, iparam, ipntr, workd,
      workl, workl.length, info)
    val w = BDV(workd)
    // ido = 99 : done flag in reverse communication
    while (ido.`val` != 99) {
      if (ido.`val` != -1 && ido.`val` != 1) {
        throw new IllegalStateException("ARPACK returns ido = " + ido.`val` +
          " This flag is not compatible with Mode 1: A*x = lambda*x, A symmetric.")
      }
      // multiply working vector with the matrix
      val inputOffset = ipntr(0) - 1
      val outputOffset = ipntr(1) - 1
      val x = w.slice(inputOffset, inputOffset + n)
      val y = w.slice(outputOffset, outputOffset + n)
      y := mul(x)
      // call ARPACK's reverse communication
      arpack.dsaupd(ido, bmat, n, which, nev.`val`, tolW, resid, ncv, v, n, iparam, ipntr,
        workd, workl, workl.length, info)
    }
    if (info.`val` != 0) {
      info.`val` match {
        case 1 => throw new IllegalStateException("ARPACK returns non-zero info = " + info.`val` +
          " Maximum number of iterations taken. (Refer ARPACK user guide for details)")
        case 2 => throw new IllegalStateException("ARPACK returns non-zero info = " + info.`val` +
          " No shifts could be applied. Try to increase NCV. " +
          "(Refer ARPACK user guide for details)")
        case _ => throw new IllegalStateException("ARPACK returns non-zero info = " + info.`val` +
          " Please refer ARPACK user guide for error message.")
      }
    }
    val d = new Array[Double](nev.`val`)
    val select = new Array[Boolean](ncv)
    // copy the Ritz vectors
    val z = java.util.Arrays.copyOfRange(v, 0, nev.`val` * n)
    // call ARPACK's post-processing for eigenvectors
    arpack.dseupd(true, "A", select, d, z, n, 0.0, bmat, n, which, nev, tol, resid, ncv, v, n,
      iparam, ipntr, workd, workl, workl.length, info)
    // number of computed eigenvalues, might be smaller than k
    val computed = iparam(4)
    val eigenPairs = java.util.Arrays.copyOfRange(d, 0, computed).zipWithIndex.map { r =>
      (r._1, java.util.Arrays.copyOfRange(z, r._2 * n, r._2 * n + n))
    }
    // sort the eigen-pairs in descending order
    val sortedEigenPairs = eigenPairs.sortBy(-_._1)
    // copy eigenvectors in descending order of eigenvalues
    val sortedU = BDM.zeros[Double](n, computed)
    sortedEigenPairs.zipWithIndex.foreach { r =>
      val b = r._2 * n
      var i = 0
      while (i < n) {
        sortedU.data(b + i) = r._1._2(i)
        i += 1
      }
    }
    (BDV[Double](sortedEigenPairs.map(_._1)), sortedU)
  }
}





