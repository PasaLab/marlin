package edu.nju.pasalab.marlin.ml


import scala.{specialized => spec}
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.math._
import scala.util.hashing._
import scala.util.{Random, Sorting}

import edu.nju.pasalab.marlin.matrix.{DenseVecMatrix, Vectors, DenseVector, DistributedMatrix}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{HashPartitioner, Partitioner}
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import breeze.linalg.{DenseMatrix => BDM, DenseVector => BDV, axpy, inv}



private[marlin]
case class OutLinkBlock(elementIds: Array[Int], shouldSend: Array[mutable.BitSet])

private[marlin]
case class InLinkBlock(elementIds: Array[Int], ratingsForBlock: Array[Array[(Array[Int], Array[Float])]])

case class Rating(user: Int, product: Int, rating: Float)


object ALSHelp {
  /** storage level for user/product in/out links */
  private val intermediateRDDStorageLevel: StorageLevel = StorageLevel.MEMORY_AND_DISK

  def ALSRun(entries: RDD[((Long, Long), Float)],
             rank:Int, iterations:Int, lambda:Double, numUserBlock:Int, numProductBlock:Int, implicitPrefs: Boolean, alpha: Double,
             seed: Long = System.nanoTime()): (DenseVecMatrix, DenseVecMatrix) = {
    val ratings = entries.map(r => new Rating(r._1._1.toInt, r._1._1.toInt, r._2))
    val sc = entries.context

    val numUserBlocks = if (numUserBlock == -1) {
      math.max(sc.defaultParallelism, ratings.partitions.size / 2)
    } else {numUserBlock}
    val numProductBlocks = if (numProductBlock == -1) {
      math.max(sc.defaultParallelism, ratings.partitions.size / 2)
    } else {numProductBlock}

    val userPartitioner = new HashPartitioner(numUserBlocks)
    val productPartitioner = new HashPartitioner(numProductBlocks)

    val ratingsByUserBlock = ratings.map { rating => (userPartitioner.getPartition(rating.user), rating)}
    val ratingsByProductBlock = ratings.map { rating => (productPartitioner.getPartition(rating.product),
      Rating(rating.product, rating.user, rating.rating))
    }

    val (userInLinks, userOutLinks) =
      ALSHelp.makeLinkRDDs(numUserBlocks, numProductBlocks, ratingsByUserBlock, productPartitioner)
    val (productInLinks, productOutLinks) =
      ALSHelp.makeLinkRDDs(numProductBlocks, numUserBlocks, ratingsByProductBlock, userPartitioner)

    val seedGen = new Random(seed)
    val seed1 = seedGen.nextInt()
    val seed2 = seedGen.nextInt()
    var users = userOutLinks.mapPartitionsWithIndex { (index, itr) =>
      val rand = new Random(byteswap32(seed1 ^ index))
      itr.map { case (x, y) =>
        (x, y.elementIds.map(_ => ALSHelp.randomFactor(rank, rand)))
      }
    }
    var products = productOutLinks.mapPartitionsWithIndex { (index, itr) =>
      val rand = new Random(byteswap32(seed2 ^ index))
      itr.map { case (x, y) =>
        (x, y.elementIds.map(_ => ALSHelp.randomFactor(rank, rand)))
      }
    }


      for (iter <- 1 to iterations) {
        products = updateFeatures(numProductBlocks, users, userOutLinks, productInLinks,
          rank, lambda, alpha,implicitPrefs, YtY = None)
        users = updateFeatures(numUserBlocks, products, productOutLinks, userInLinks,
          rank, lambda, alpha, implicitPrefs, YtY = None)
      }


    val usersOut = unblockFactors(users, userOutLinks)
    val productsOut = unblockFactors(products, productOutLinks)

    products.unpersist()

    // Clean up.
    userInLinks.unpersist()
    userOutLinks.unpersist()
    productInLinks.unpersist()
    productOutLinks.unpersist()
    (usersOut, productsOut)
  }
  /**
   * Make the out-links table for a block of the users (or products) dataset given the list of
   * (user, product, rating) values for the users in that block (or the opposite for products).
   */
  private def makeOutLinkBlock(numProductBlocks: Int, ratings: Array[Rating],
                               productPartitioner: Partitioner): OutLinkBlock = {
    val userIds = ratings.map(_.user).distinct.sorted
    val numUsers = userIds.length
    val userIdToPos = userIds.zipWithIndex.toMap
    val shouldSend = Array.fill(numUsers)(new mutable.BitSet(numProductBlocks))
    for (r <- ratings) {
      shouldSend(userIdToPos(r.user))(productPartitioner.getPartition(r.product)) = true
    }
    OutLinkBlock(userIds, shouldSend)
  }

  /**
   * Make the in-links table for a block of the users (or products) dataset given a list of
   * (user, product, rating) values for the users in that block (or the opposite for products).
   */
  private def makeInLinkBlock(numProductBlocks: Int, ratings: Array[Rating],
                              productPartitioner: Partitioner): InLinkBlock = {
    val userIds = ratings.map(_.user).distinct.sorted
    val userIdToPos = userIds.zipWithIndex.toMap
    // Split out our ratings by product block
    val blockRatings = Array.fill(numProductBlocks)(new ArrayBuffer[Rating])
    for (r <- ratings) {
      blockRatings(productPartitioner.getPartition(r.product)) += r
    }
    val ratingsForBlock = new Array[Array[(Array[Int], Array[Float])]](numProductBlocks)
    for (productBlock <- 0 until numProductBlocks) {
      // Create an array of (product, Seq(Rating)) ratings
      val groupedRatings = blockRatings(productBlock).groupBy(_.product).toArray
      // Sort them by product ID
      val ordering = new Ordering[(Int, ArrayBuffer[Rating])] {
        def compare(a: (Int, ArrayBuffer[Rating]), b: (Int, ArrayBuffer[Rating])): Int =
          a._1 - b._1
      }
      Sorting.quickSort(groupedRatings)(ordering)
      // Translate the user IDs to indices based on userIdToPos
      ratingsForBlock(productBlock) = groupedRatings.map { case (p, rs) =>
        (rs.view.map(r => userIdToPos(r.user)).toArray, rs.view.map(_.rating).toArray)
      }
    }
    InLinkBlock(userIds, ratingsForBlock)
  }

  /**
   * Make RDDs of InLinkBlocks and OutLinkBlocks given an RDD of (blockId, (u, p, r)) values for
   * the users (or (blockId, (p, u, r)) for the products). We create these simultaneously to avoid
   * having to shuffle the (blockId, (u, p, r)) RDD twice, or to cache it.
   */
  def makeLinkRDDs(numUserBlocks: Int,
                   numProductBlocks: Int,
                   ratingsByUserBlock: RDD[(Int, Rating)],
                   productPartitioner: Partitioner): (RDD[(Int, InLinkBlock)], RDD[(Int, OutLinkBlock)]) = {
    val grouped = ratingsByUserBlock.partitionBy(new HashPartitioner(numUserBlocks))
    val links = grouped.mapPartitionsWithIndex((blockId, elements) => {
      val ratings = elements.map(_._2).toArray
      val inLinkBlock = makeInLinkBlock(numProductBlocks, ratings, productPartitioner)
      val outLinkBlock = makeOutLinkBlock(numProductBlocks, ratings, productPartitioner)
      Iterator.single((blockId, (inLinkBlock, outLinkBlock)))
    }, preservesPartitioning = true)
    val inLinks = links.mapValues(_._1)
    val outLinks = links.mapValues(_._2)
    inLinks.persist(intermediateRDDStorageLevel)
    outLinks.persist(intermediateRDDStorageLevel)
    (inLinks, outLinks)
  }

  /**
   * Make a random factor vector with the given random.
   */
  def randomFactor(rank: Int, rand: Random): Array[Double] = {
    // Choose a unit vector uniformly at random from the unit sphere, but from the
    // "first quadrant" where all elements are nonnegative. This can be done by choosing
    // elements distributed as Normal(0,1) and taking the absolute value, and then normalizing.
    // This appears to create factorizations that have a slightly better reconstruction
    // (<1%) compared picking elements uniformly at random in [0,1].
    val factor = Array.fill(rank)(abs(rand.nextGaussian()))
    val norm = sqrt(factor.map(x => x * x).sum)
    factor.map(x => x / norm)
  }

  /**
   * Computes the (`rank x rank`) matrix `YtY`, where `Y` is the (`nui x rank`) matrix of factors
   * for each user (or product), in a distributed fashion.
   *
   * @param factors the (block-distributed) user or product factor vectors
   * @return YtY - whose value is only used in the implicit preference model
   */
  def computeYtY(factors: RDD[(Int, Array[Array[Double]])], rank:Int) = {
    val n = rank * (rank + 1) / 2
    val LYtY = factors.values.aggregate(new BDM[Double](n, 1))( seqOp = (L, Y) => {
      Y.foreach(y => dspr(1.0, wrapDoubleArray(y), L))
      L
    }, combOp = (L1, L2) => {
      //L1.addi(L2)
      L1 += L2
    })
    val YtY = new BDM[Double](rank, rank)
    fillFullMatrix(LYtY, YtY)
    YtY
  }

  /**
   * Given a triangular matrix in the order of fillXtX above, compute the full symmetric square
   * matrix that it represents, storing it into destMatrix.
   */
  private def fillFullMatrix(triangularMatrix: BDM[Double], destMatrix: BDM[Double]) {
    val rank = destMatrix.rows
    var i = 0
    var pos = 0
    while (i < rank) {
      var j = 0
      while (j <= i) {
        destMatrix.data(i*rank + j) = triangularMatrix.data(pos)
        destMatrix.data(j*rank + i) = triangularMatrix.data(pos)
        pos += 1
        j += 1
      }
      i += 1
    }
  }

  /**
   * Wrap a double array in a DoubleMatrix without creating garbage.
   * This is a temporary fix for jblas 1.2.3; it should be safe to move back to the
   * DoubleMatrix(double[]) constructor come jblas 1.2.4.
   */
  private def wrapDoubleArray(v: Array[Double]): BDM[Double] = {
    new BDM[Double](v.length, 1, v, 0)
  }

  /**
   * Adds alpha * x * x.t to a matrix in-place. This is the same as BLAS's DSPR.
   *
   * @param L the lower triangular part of the matrix packed in an array (row major)
   */
  def dspr(alpha: Double, x: BDM[Double], L: BDM[Double]) = {
    val n = x.rows * x.cols
    var i = 0
    var j = 0
    var idx = 0
    var axi = 0.0
    val xd = x.data
    val Ld = L.data
    while (i < n) {
      axi = alpha * xd(i)
      j = 0
      while (j <= i) {
        Ld(idx) += axi * xd(j)
        j += 1
        idx += 1
      }
      i += 1
    }
  }

  /**
   * Compute the user feature vectors given the current products (or vice-versa). This first joins
   * the products with their out-links to generate a set of messages to each destination block
   * (specifically, the features for the products that user block cares about), then groups these
   * by destination and joins them with the in-link info to figure out how to update each user.
   * It returns an RDD of new feature vectors for each user block.
   */
  def updateFeatures(numUserBlocks: Int,
                     products: RDD[(Int, Array[Array[Double]])],
                     productOutLinks: RDD[(Int, OutLinkBlock)],
                     userInLinks: RDD[(Int, InLinkBlock)],
                     rank: Int,
                     lambda: Double,
                     alpha: Double,
                     implicitPrefs:Boolean,
                     YtY: Option[Broadcast[BDM[Double]]]): RDD[(Int, Array[Array[Double]])] = {
    val tmp = productOutLinks.join(products).flatMap { case (bid, (outLinkBlock, factors)) =>
      val toSend = Array.fill(numUserBlocks)(new ArrayBuffer[Array[Double]])
      for (p <- 0 until outLinkBlock.elementIds.length; userBlock <- 0 until numUserBlocks) {
        if (outLinkBlock.shouldSend(p)(userBlock)) {
          toSend(userBlock) += factors(p)
        }
      }
      toSend.zipWithIndex.map{ case (buf, idx) => (idx, (bid, buf.toArray))  }
    }
    val tmp1 = tmp.groupByKey(new HashPartitioner(numUserBlocks))
    val tmp2 = tmp1.join(userInLinks)
    tmp2.mapValues{ case (messages, inLinkBlock) =>
      updateBlock(messages, inLinkBlock, rank, lambda, alpha, implicitPrefs, YtY)
    }
  }

  /**
   * Compute the new feature vectors for a block of the users matrix given the list of factors
   * it received from each product and its InLinkBlock.
   */
  def updateBlock(messages: Iterable[(Int, Array[Array[Double]])], inLinkBlock: InLinkBlock,
                  rank: Int, lambda: Double, alpha: Double, implicitPrefs:Boolean, YtY: Option[Broadcast[BDM[Double]]])
  : Array[Array[Double]] =
  {
    // Sort the incoming block factor messages by block ID and make them an array
    val blockFactors = messages.toSeq.sortBy(_._1).map(_._2).toArray // Array[Array[Double]]
  val numProductBlocks = blockFactors.length
    val numUsers = inLinkBlock.elementIds.length

    // We'll sum up the XtXes using vectors that represent only the lower-triangular part, since
    // the matrices are symmetric
    val triangleSize = rank * (rank + 1) / 2
    val t1 = new BDM(triangleSize, 1, new Array[Double](triangleSize))
    val userXtX = Array.fill(numUsers)(t1)
    val t2 = new BDM(rank, 1, new Array[Double](rank))
    val userXy = Array.fill(numUsers)(t2)


    // Some temp variables to avoid memory allocation
    val tempXtX = BDM.zeros[Double](triangleSize, 1)
    val fullXtX = BDM.zeros[Double](rank, rank)

    // Count the number of ratings each user gives to provide user-specific regularization
    val numRatings = Array.fill(numUsers)(0)

    // Compute the XtX and Xy values for each user by adding products it rated in each product
    // block
    for (productBlock <- 0 until numProductBlocks) {
      var p = 0
      while (p < blockFactors(productBlock).length) {
        val y = blockFactors(productBlock)(p)
        val x = wrapDoubleArray(y)
        //tempXtX.fill(0.0)
        for(i <- 0 until tempXtX.rows; j <- 0 until tempXtX.cols) {
          tempXtX.update((i, j), 0.0)
        }
        dspr(1.0, x, tempXtX)
        val (us, rs) = inLinkBlock.ratingsForBlock(productBlock)(p)
        if (implicitPrefs) {
          var i = 0
          while (i < us.length) {
            numRatings(us(i)) += 1
            // Extension to the original paper to handle rs(i) < 0. confidence is a function
            // of |rs(i)| instead so that it is never negative:
            val confidence = 1 + alpha * abs(rs(i).asInstanceOf[Double])
            axpy(confidence - 1.0, tempXtX, userXtX(us(i)))
            // For rs(i) < 0, the corresponding entry in P is 0 now, not 1 -- negative rs(i)
            // means we try to reconstruct 0. We add terms only where P = 1, so, term below
            // is now only added for rs(i) > 0:
            if (rs(i).asInstanceOf[Double] > 0) {
              axpy(confidence, x, userXy(us(i)))
            }
            i += 1
          }
        } else {
          var i = 0
          while (i < us.length) {
            numRatings(us(i)) += 1
            //userXtX(us(i)).addi(tempXtX)
            userXtX(us(i)) += tempXtX
            axpy(rs(i).asInstanceOf[Double], x, userXy(us(i)))
            i += 1
          }
        }
        p += 1
      }
    }

    val ws = null

    // Solve the least-squares problem for each user and return the new feature vectors
    Array.range(0, numUsers).map { index =>
      // Compute the full XtX matrix from the lower-triangular part we got above
      fillFullMatrix(userXtX(index), fullXtX)
      // Add regularization
      val regParam = numRatings(index) * lambda
      var i = 0
      while (i < rank) {
        fullXtX.data(i * rank + i) += regParam
        i += 1
      }
      // Solve the resulting matrix, which is symmetric and positive-definite
      if (implicitPrefs) {
        //solveLeastSquares(fullXtX.addi(YtY.get.value), userXy(index))
        fullXtX += YtY.get.value
        solveLeastSquares(fullXtX, userXy(index))
      } else {
        solveLeastSquares(fullXtX, userXy(index))
      }
    }
  }

  /**
   * Given A^T A and A^T b, find the x minimising ||Ax - b||_2, possibly subject
   * to nonnegativity constraints if `nonnegative` is true.
   */
  def solveLeastSquares(ata: BDM[Double], atb: BDM[Double]): Array[Double] = {
    val q = inv(ata)
    val m:BDM[Double] = q * atb
    m.data
  }

  /**
   * Flatten out blocked user or product factors into an RDD of (id, factor vector) pairs
   */
  def unblockFactors(blockedFactors: RDD[(Int, Array[Array[Double]])],
                     outLinks: RDD[(Int, OutLinkBlock)]): DenseVecMatrix = {
    val result = blockedFactors.join(outLinks).flatMap { case (b, (factors, outLinkBlock)) =>
      for (i <- 0 until factors.length) yield (outLinkBlock.elementIds(i).toLong, BDV(factors(i)))
    }
    new DenseVecMatrix(result)
  }
}