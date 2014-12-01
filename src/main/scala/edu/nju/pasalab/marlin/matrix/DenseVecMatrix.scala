package edu.nju.pasalab.marlin.matrix

import java.io.IOException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.collection.mutable.ArrayBuffer
import scala.collection.parallel.mutable.ParArray

import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV}
import org.apache.log4j.{Logger, Level}
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.hadoop.io.{Text, NullWritable}
import org.apache.hadoop.mapred.TextOutputFormat

import edu.nju.pasalab.marlin.utils.MTUtils

/**
 * This class overrides from [[org.apache.spark.mllib.linalg.distributed.IndexedRowMatrix]]
 * Notice: some code in this file is copy from MLlib to make it compatible
 */
class DenseVecMatrix(
    val rows: RDD[(Long, DenseVector)],
    private var nRows: Long,
    private var nCols: Long) extends DistributedMatrix{

  private var resultCols:Long = 0
  def this(rows: RDD[(Long, DenseVector)]) = this(rows, 0L, 0)

  def this(sc: SparkContext , array: Array[Array[Double]] , partitions: Int = 2){
    this( sc.parallelize(array.zipWithIndex.
      map{ case(t,i)  => (i.toLong, Vectors.dense(t)) }, partitions) )
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

  /** Collects data and assembles a local dense breeze matrix (for test only). */
  override private [matrix]  def toBreeze(): BDM[Double] = {
    val m = numRows().toInt
    val n = numCols().toInt
    val mat = BDM.zeros[Double](m, n)
    rows.collect().foreach { case (rowIndex, vector) =>
      val i = rowIndex.toInt
      vector.toBreeze.activeIterator.foreach { case (j, v) =>
        mat(i, j) = v
      }
    }
    mat
  }

  /**
   * This function is used to satisfy the
   * @param other
   * @param cores
   * @return
   */
   def multiply(other: DistributedMatrix, cores: Int): BlockMatrix = {
    multiply(other, cores, 300)
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

  final def multiply(other: DistributedMatrix,
            cores: Int,
            broadcastThreshold: Int = 300): BlockMatrix = {
    other match {
      case that: DenseVecMatrix => {
        require(numCols == that.numRows(),
          s"Dimension mismatch: ${numCols()} vs ${that.numRows()}")

        val broadSize = broadcastThreshold * 1024 * 1024 / 8
        if (that.numRows() * that.numCols() <= broadSize) {
          val parallelism = math.min(8 * cores, numRows() / 2).toInt
          multiplyBroadcast(that, parallelism, (parallelism, 1, 1), "broadcastB")
        } else if (numRows() * numCols() <= broadSize) {
          val parallelism = math.min(8 * cores, that.numRows() / 2).toInt
          multiplyBroadcast(that, parallelism, (1, 1, parallelism), "broadcastA")
        } else if (0.8 < (numRows() * that.numCols()).toDouble / (numCols() * numCols()).toDouble
          && (numRows() * that.numCols()).toDouble / (numCols() * numCols()).toDouble < 1.2
          && numRows() / numCols() < 1.2
          && numRows() / numCols() > 0.8) {
          multiplyHama(that, math.floor(math.pow(3 * cores, 1.0 / 3.0)).toInt)
        } else {
          multiplyCarma(that, cores)
        }
      }
      case that: BlockMatrix => {
        val broadSize = broadcastThreshold * 1024 * 1024 / 8
        if (that.numRows() * that.numCols() <= broadSize && that.numBlksByRow() == 1){
          val broadBDM = rows.context.broadcast(this.toBreeze())
          val result = that.blocks.mapPartitions(iter => {
            iter.map( t =>{
              (t._1, (broadBDM.value * t._2).asInstanceOf[BDM[Double]])
            })
          })
          new BlockMatrix(result, numRows(), that.numCols(), that.numBlksByRow(), that.numBlksByCol())
        }else {
          multiply(that.toDenseVecMatrix(), cores)
        }
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
      case that: DenseVecMatrix => {
        val result = rows.join(that.rows).map(t => {
          (t._1, new DenseVector(t._2._1.toArray++:t._2._2.toArray))
        })
        new DenseVecMatrix(result, numRows(), numCols() + that.numCols())
      }
      case that: BlockMatrix => {
        val thatDenVec = that.toDenseVecMatrix()
        cBind(thatDenVec)
      }
      case that: DistributedMatrix => {
        throw new IllegalArgumentException("Do not support this type " + that.getClass + " for cBind operation" )
      }
    }
  }

  /**
   * A matrix multiply another DenseVecMatrix
   *
   * @param other another matrix in DenseVecMatrix format
   * @param blkNum is the split nums of submatries, if you set it as 10,
   *               which means you split every original large matrix into 10*10=100 blocks.
   *               The smaller this argument, the biger every worker get submatrix.
   * @return a distributed matrix in BlockMatrix type
   */
  private[marlin] def multiplyHama(other: DenseVecMatrix, blkNum: Int): BlockMatrix = {
    val otherRows = other.numRows()
    require(numCols == otherRows, s"Dimension mismatch: ${numCols} vs ${otherRows}")

    resultCols = other.numCols()

    val thisBlocks = toBlockMatrix(blkNum, blkNum)
    val otherBlocks = other.toBlockMatrix(blkNum, blkNum)
    thisBlocks.multiply(otherBlocks, blkNum * blkNum * blkNum)
  }


  /**
   * refer to CARMA, implement the dimension-split ways
   *
   * @param other matrix to be multiplied, in the form of DenseVecMatrix
   * @param cores all the num of cores cross the cluster
   * @return a distributed matrix in BlockMatrix type
   */

  private[marlin] def multiplyCarma(other: DenseVecMatrix, cores: Int): BlockMatrix = {
    val otherRows = other.numRows()
    require(numCols == otherRows, s"Dimension mismatch: ${numCols} vs ${otherRows}")
    val (mSplitNum, kSplitNum, nSplitNum) =
      MTUtils.splitMethod(numRows(), numCols(), other.numCols(), cores)
    val thisCollects = toBlockMatrix(mSplitNum, kSplitNum)
    val otherCollects = other.toBlockMatrix(kSplitNum, nSplitNum)
    thisCollects.multiply(otherCollects, cores)
  }


  /**
   * refer to CARMA, implement the dimension-split ways
   *
   * @param other matrix to be multiplied, in the form of DenseVecMatrix
   * @param parallelism all the num of cores cross the cluster
   * @param mode whether broadcast A or B
   * @return
   */

  private[marlin] def multiplyBroadcast(other: DenseVecMatrix,
                              parallelism: Int,
                              splits:(Int, Int, Int), mode: String): BlockMatrix = {
    val otherRows = other.numRows()
    require(numCols == otherRows, s"Dimension mismatch: ${numCols} vs ${otherRows}")
    val thisCollects = toBlockMatrix(splits._1, splits._2)
    val otherCollects = other.toBlockMatrix(splits._2, splits._3)
    thisCollects.multiplyBroadcast(otherCollects, parallelism, splits, mode)
  }


  /**
   * This function is still in progress. it needs to do more work
   * LU decompose this DenseVecMatrix to generate a lower triangular matrix L
   * and a upper triangular matrix U
   *
   * @return a pair (lower triangular matrix, upper triangular matrix)
   */
  def luDecompose(mode: String = "auto"): (DenseVecMatrix, DenseVecMatrix) = {
    val iterations = numRows
    require(iterations == numCols,
      s"currently we only support square matrix: ${iterations} vs ${numCols}")
    if (!rows.context.getCheckpointDir.isDefined){
      println("Waning, checkpointdir is not set! We suggest you set it before running luDecopose")
    }
    // object LUmode extends Enumeration {
    // val LocalBreeze, DistSpark = Value
    // }
    // val computeMode = mode match {
    // case "auto" => if ( iterations > 10000L){
    // LUmode.DistSpark
    // }else {
    // LUmode.LocalBreeze
    // }
    // case "breeze" => LUmode.LocalBreeze
    // case "dist" => LUmode.DistSpark
    // case _ => throw new IllegalArgumentException(s"Do not support mode $mode.")
    // }
    //
    // val (lower: IndexMatrix, upper: IndexMatrix) = computeMode match {
    // case LUmode.LocalBreeze =>
    // val temp = bLU(toBreeze())
    // Matrices.fromBreeze(breeze.linalg.lowerTriangular(temp._1))
    // }
    //
    //copy construct a IndexMatrix to maintain the original matrix
    var matr = new DenseVecMatrix(rows.map(t => {
      val array = Array.ofDim[Double](numCols().toInt)
      val v = t._2.toArray
      for (k <- 0 until v.length) {
        array(k) = v.apply(k)
      }
      (t._1, Vectors.dense(array))
    }))
    val num = iterations.toInt
    var lowerMat = MTUtils.zerosDenVecMatrix(rows.context, numRows(), numCols().toInt)
    for (i <- 0 until num) {
      val vector = matr.rows.filter(t => t._1.toInt == i).map(t => t._2).first()
      val c = matr.rows.context.broadcast(vector.apply(i))
      val broadVec = matr.rows.context.broadcast(vector)
      //TODO: here we omit the compution of L
      //TODO: here collect() is too much cost, find another method
      val lupdate = matr.rows.map( t => (t._1 , t._2.toArray.apply(i) / c.value)).collect()
      val updateVec = Array.ofDim[Double](num)
      for ( l <- lupdate){
        updateVec.update(l._1.toInt , l._2)
      }
      val broadLV = matr.rows.context.broadcast(updateVec)
      val lresult = lowerMat.rows.mapPartitions( iter => {
        iter.map { t =>
          if ( t._1.toInt >= i) {
            val vec = t._2.toArray
            vec.update(i, broadLV.value.apply(t._1.toInt))
            (t._1, Vectors.dense(vec))
          }else t
        }}, true)
      lowerMat = new DenseVecMatrix(lresult, numRows(), numCols())
      //cache the lower matrix to speed the compution
      val result = matr.rows.mapPartitions(iter =>{
        iter.map(t => {
          if ( t._1.toInt > i){
            val vec = t._2.toArray
            val lupdate = vec.apply(i) / c.value
            val mfactor = -vec.apply(i) / c.value
            for (k <- 0 until vec.length) {
              vec.update(k, vec.apply(k) + mfactor * broadVec.value.apply(k))
            }
            (t._1, Vectors.dense(vec))
          }
          else t
        })}, true)
      matr = new DenseVecMatrix(result, numRows(), numCols())
      //cache the matrix to speed the compution
      matr.rows.cache()
      if (i % 2000 == 0){
        if (matr.rows.context.getCheckpointDir.isDefined)
          matr.rows.checkpoint()
      }
    }
    (lowerMat, matr)
  }


  /**
   * This matrix add another DistributedMatrix
   *
   * @param other another matrix in DenseVecMatrix format
   */
  final def add(other: DistributedMatrix): DenseVecMatrix = {
    other match {
      case that: DenseVecMatrix => {
        require(numRows() == that.numRows(), s"Dimension mismatch: ${numRows()} vs ${that.numRows()}")
        require(numCols() == that.numCols, s"Dimension mismatch: ${numCols()} vs ${that.numCols()}")

        val result = rows.join(that.rows).map(t =>
          (t._1, Vectors.fromBreeze((t._2._1.toBreeze + t._2._2.toBreeze).asInstanceOf[BDV[Double]])))
        new DenseVecMatrix(result, numRows(), numCols())
      }
      case that: BlockMatrix => {
        add(that.toDenseVecMatrix())
      }
      case that: DistributedMatrix => {
        throw new IllegalArgumentException("Do not support this type " + that.getClass + "for add operation" )
      }
    }

  }

  /**
   * This matrix minus another DistributedMatrix
   *
   * @param other another matrix in DenseVecMatrix format
   */
  final def subtract(other: DistributedMatrix): DenseVecMatrix = {
    other match {
      case that: DenseVecMatrix => {
        require(numRows() == that.numRows(), s"Dimension mismatch: ${numRows()} vs ${other.numRows()}")
        require(numCols == that.numCols, s"Dimension mismatch: ${numCols()} vs ${other.numCols()}")

        val result = rows.join(that.rows).map(t =>
          (t._1, Vectors.fromBreeze((t._2._1.toBreeze - t._2._2.toBreeze).asInstanceOf[BDV[Double]])))
        new DenseVecMatrix(result, numRows(), numCols())
      }
      case that: BlockMatrix => {
        subtract(that.toDenseVecMatrix())
      }
    }

  }


  /**
   * Element in this matrix element-wise add another scalar
   *
   * @param b the number to be element-wise added
   */
  final def add(b: Double): DenseVecMatrix = {
    val result = rows.map(t =>(t._1, Vectors.dense(t._2.toArray.map(_ + b))))
    new DenseVecMatrix(result, numRows(), numCols())
  }

  /**
   * Element in this matrix element-wise minus another scalar
   *
   * @param b a number to be element-wise subtracted
   */
  final def subtract(b: Double): DenseVecMatrix = {
    val result = rows.map(t =>(t._1, Vectors.dense(t._2.toArray.map(_ - b))))
    new DenseVecMatrix(result, numRows(), numCols())
  }

  /**
   * Element in this matrix element-wise minus by another scalar
   *
   * @param b a number in the format of double
   */
  final def subtractBy(b: Double): DenseVecMatrix = {
    val result = rows.map(t =>(t._1, Vectors.dense(t._2.toArray.map(b - _ ))))
    new DenseVecMatrix(result, numRows(), numCols())
  }

  /**
   * Element in this matrix element-wise multiply another scalar
   *
   * @param b a number in the format of double
   */
  final def multiply(b: Double): DenseVecMatrix = {
    val result = rows.map(t =>(t._1, Vectors.dense(t._2.toArray.map(_ * b))))
    new DenseVecMatrix(result, numRows(), numCols())
  }

  /**
   * Element in this matrix element-wise divide another scalar
   *
   * @param b a number in the format of double
   * @return result in DenseVecMatrix type
   */
  final def divide(b: Double): DenseVecMatrix = {
    val result = rows.map(t =>(t._1, Vectors.dense(t._2.toArray.map( _ / b))))
    new DenseVecMatrix(result, numRows(), numCols())
  }

  /**
   * Element in this matrix element-wise divided by another scalar
   *
   * @param b a number in the format of double
   */
  final def divideBy(b: Double): DenseVecMatrix = {
    val result = rows.map(t =>(t._1, Vectors.dense(t._2.toArray.map( b / _))))
    new DenseVecMatrix(result, numRows(), numCols())
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

    new DenseVecMatrix(rows.filter(t => (t._1 >= startRow && t._1 <= endRow)).map(t => (t._1 - startRow, t._2))
      , endRow - startRow + 1, numCols())
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

    new DenseVecMatrix(rows.map(t => (t._1, Vectors.dense(t._2.toArray.slice(startCol, endCol + 1))))
      , numRows(), endCol - startCol + 1)
  }

  /**
   * get sub matrix according to the given range of column
   *
   * @param startRow the start row of the subMatrix, this row is included
   * @param endRow the end row of the subMatrix, this row is included
   * @param startCol the start column of the subMatrix, this column is included
   * @param endCol the end column of the subMatrix, this column is included
   */
  final def getSubMatrix(startRow: Long, endRow: Long ,startCol: Int, endCol: Int): DenseVecMatrix = {
    require((startRow >= 0 && endRow <= numRows()), s"start row or end row dismatch the matrix num of rows")
    require((startCol >= 0 && endCol <= numCols()),
      s"start column or end column dismatch the matrix num of columns")

    new DenseVecMatrix(rows
      .filter(t => (t._1 >= startRow && t._1 <= endRow))
      .map(t => (t._1 - startRow, Vectors.dense(t._2.toArray.slice(startCol,endCol + 1))))
    , endRow - startRow + 1, endCol - startCol + 1)
  }

  /**
   * compute the norm of this matrix
   *
   * @param mode the same with Matlab operations,
   *             `1` means get the 1-norm, the largest column sum of matrix
   *             `2` means get the largest singular value, the default mode --- need to do
   *             `inf` means the infinity norm, the largest row sum of matrix
   *             `fro` means the Frobenius-norm of matrix -- need to do
   */
  final def norm(mode: String): Double = {
    mode match {
      case "1" => {
        rows.mapPartitions( iter =>{
          val columnAcc = new ParArray[Double](numCols().toInt)
          iter.map( t => {
            val arr = t._2.toArray
            for ( i <- 0 until arr.length){
              columnAcc(i) += math.abs(arr(i))
            }
          }
         )
        columnAcc.toIterator}).max()
      }
//      case "2" => {
//
//      }
      case "inf" => {
        rows.map( t => t._2.toArray.reduce( _ + _ )).max()
      }
//      case "for" => {
//
//      }
    }
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

  /**
   * A simple wraper of saving the DenseVecMatrix to SequenceFile
   *
   * @param path matrix file path
   */
  def saveSequenceFile(path: String) {
    rows.saveAsSequenceFile(path)
  }

  /**
   * transform the DenseVecMatrix to BlockMatrix
   *
   * @param numByRow num of subMatrix along the row side set by user,
   *                 the actual num of subMatrix along the row side is `blksByRow`
   * @param numByCol num of subMatrix along the column side set by user,
   *                 the actual num of subMatrix along the row side is `blksByCol`
   * @return the transformated matrix in BlockMatrix type
   */

  def toBlockMatrix(numByRow: Int, numByCol: Int): BlockMatrix = {

      val mRows = numRows().toInt
      val mColumns = numCols().toInt
      val mBlockRowSize = math.ceil(mRows.toDouble / numByRow.toDouble).toInt
      val mBlockColSize = math.ceil(mColumns.toDouble / numByCol.toDouble).toInt
      val blksByRow = math.ceil(mRows.toDouble / mBlockRowSize).toInt
      val blksByCol = math.ceil(mColumns.toDouble / mBlockColSize).toInt
    if (blksByCol == 1){
     val result = rows.mapPartitions(iter => {
        iter.map( t => {
          (t._1.toInt / mBlockRowSize, t)})
      }, true).groupByKey().mapPartitions(iter => {
        iter.map(t => {
          val blockRow = t._1
          val rowLen = if ((blockRow + 1) * mBlockRowSize > numRows()){
            numRows().toInt - blockRow * mBlockRowSize
          }else {mBlockRowSize}
          val iterator = t._2.iterator
          val mat = BDM.zeros[Double](rowLen, mBlockColSize)
          while (iterator.hasNext){
            val (index, vec) = iterator.next()
            vec.toArray.zipWithIndex.map(x =>  {
              mat.update(index.toInt - blockRow * mBlockRowSize, x._2, x._1)
            })
          }
          (new BlockID(t._1, 0), mat)
        })
      })
      new BlockMatrix(result, numRows(), numCols(), blksByRow, blksByCol)
    }else {
      val result = rows.mapPartitions(iter => {
        iter.flatMap(t => {
          var startColumn = 0
          var endColumn = 0
          var arrayBuf = new ArrayBuffer[(BlockID, (Long, Vector))]

          val elems = t._2.toArray
          var i = 0
          while (endColumn < (mColumns - 1)) {
            startColumn = i * mBlockColSize
            endColumn = startColumn + mBlockColSize - 1
            if (endColumn >= mColumns) {
              endColumn = mColumns - 1
            }

            val vector = new Array[Double](endColumn - startColumn + 1)
            for (j <- startColumn to endColumn) {
              vector(j - startColumn) = elems(j)
            }

            arrayBuf += ((new BlockID(t._1.toInt / mBlockRowSize, i), (t._1, Vectors.dense(vector))))
            i += 1
          }
          arrayBuf
        })
      })
        .groupByKey()
        .mapPartitions(a => {
        a.map(
          input => {
            val colBase = input._1.column * mBlockColSize
            val rowBase = input._1.row * mBlockRowSize

            //the block's size: rows & columns
            var smRows = mBlockRowSize
            if ((rowBase + mBlockRowSize - 1) >= mRows) {
              smRows = mRows - rowBase
            }

            var smCols = mBlockColSize
            if ((colBase + mBlockColSize - 1) >= mColumns) {
              smCols = mColumns - colBase
            }

            val itr = input._2.iterator
            //to generate the local matrix, be careful, the array is column major
            val array = Array.ofDim[Double](smRows * smCols)

            while (itr.hasNext) {
              val vec = itr.next()
              if (vec._2.size != smCols) {
                Logger.getLogger(getClass).
                  log(Level.ERROR, "vectors:  " + input._2 + "Block Column Size mismatch")
                throw new IOException("Block Column Size mismatch")
              }

              val rowOffset = vec._1.toInt - rowBase
              if (rowOffset >= smRows || rowOffset < 0) {
                Logger.getLogger(getClass).log(Level.ERROR, "Block Row Size mismatch")
                throw new IOException("Block Row Size mismatch")
              }

              val tmp = vec._2.toArray
              for (i <- 0 until tmp.length) {
                array(i * smRows + rowOffset) = tmp(i)
              }
            }

            val subMatrix = new BDM(smRows, smCols, array)
            (input._1, subMatrix)
          })
      }, true)

      new BlockMatrix(result, numRows(), numCols(), blksByRow, blksByCol)
    }
  }

  /**
   * transform the DenseVecMatrix to SparseVecMatrix
   */
  def toSparseVecMatrix(): SparseVecMatrix = {
    val result = rows.mapPartitions( iter => {
      iter.map( t => {
        val array = t._2.toArray
        val indices = new ArrayBuffer[Int]()
        val values = new ArrayBuffer[Double]()
        for (i <- 0 until array.length){
          if (array(i) != 0){
            indices += i
            values += array(i)
          }
        }
        if (indices.size >= 0) {
          (t._1, new SparseVector(indices.size, indices.toArray, values.toArray))
        }else {throw new IllegalArgumentException("indices size is empty")}
      })
    })
    new SparseVecMatrix(result, numRows(), numCols())
  }

  /**
   * print the matrix out
   */
  def print() {
    if (numRows() > 20){
      rows.take(20).foreach( t => println("index: "+t._1 + ", vector: "+ t._2.print(8)))
      println((numRows() - 20)+ "rows more...")
    }else {
      rows.collect().foreach(t => println("index: "+t._1 + ", vector: "+ t._2.print(8)))
    }
  }

  /**
   * A transpose view of this matrix
   * @return
   */
  def transpose(): BlockMatrix = {
    require(numRows() < Int.MaxValue, s"the row length of matrix is too large to transpose")
    val sc = rows.context
    val blkByRow = if (!sc.getConf.getOption("spark.default.parallelism").isEmpty) {
      sc.getConf.get("spark.default.parallelism").toInt
    }else {
      sc.defaultMinPartitions
    }
    toBlockMatrix(math.min(blkByRow, numRows().toInt / 2), 1).transpose()
  }

}
