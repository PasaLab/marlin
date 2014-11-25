package edu.nju.pasalab.marlin.utils

import java.util.Random

import cern.jet.random.Poisson
import cern.jet.random.engine.DRand
import org.apache.spark.util.random.Pseudorandom

/**
 * Trait for random data generators that generate i.i.d. data.
 */
trait RandomDataGenerator[T] extends Pseudorandom with Serializable {

  /**
   * Returns an i.i.d. sample as a generic type from an underlying distribution.
   */
  def nextValue(): T

  /**
   * Returns a copy of the RandomDataGenerator with a new instance of the rng object used in the
   * class when applicable for non-locking concurrent usage.
   */
  def copy(): RandomDataGenerator[T]
}

/**
 * Generate all zero data
 */
class ZerosGenerator() extends RandomDataGenerator[Double] {

  override def nextValue(): Double = 0.0

  override def copy(): RandomDataGenerator[Double] = this

  override def setSeed(seed: Long): Unit = {}
}

/**
 * Generate all one data
 */
class OnesGenerator() extends RandomDataGenerator[Double] {

  override def nextValue(): Double = 1.0

  override def copy(): RandomDataGenerator[Double] = this

  override def setSeed(seed: Long): Unit = {}
}

/**
 * Generates i.i.d. samples from U[start, end] (default is U[0.0, 1.0])
 */
class UniformGenerator(start: Double = 0.0, end: Double = 1.0) extends RandomDataGenerator[Double] {

  // XORShiftRandom for better performance. Thread safety isn't necessary here.
  private val random = new XORShiftRandom()

  override def nextValue(): Double = {
    (end - start)*random.nextDouble() + start
  }

  override def setSeed(seed: Long) = random.setSeed(seed)

  override def copy(): UniformGenerator = new UniformGenerator(start, end)
}

/**
 * Generates i.i.d. samples from the normal distribution N[mean, variance] (default is N[0,1]).
 */
class StandardNormalGenerator(mean: Double = 0.0, variance: Double = 1.0) extends RandomDataGenerator[Double] {

  // XORShiftRandom for better performance. Thread safety isn't necessary here.
  private val random = new XORShiftRandom()

  override def nextValue(): Double = {
    mean + math.sqrt(variance) * random.nextGaussian()
  }

  override def setSeed(seed: Long) = random.setSeed(seed)

  override def copy(): StandardNormalGenerator = new StandardNormalGenerator(mean, variance)
}

/**
 * Generates i.i.d. samples from the Poisson distribution with the given mean.
 *
 * @param mean mean for the Poisson distribution.
 */
class PoissonGenerator(val mean: Double) extends RandomDataGenerator[Double] {

  private var rng = new Poisson(mean, new DRand)

  override def nextValue(): Double = rng.nextDouble()

  override def setSeed(seed: Long) {
    rng = new Poisson(mean, new DRand(seed.toInt))
  }

  override def copy(): PoissonGenerator = new PoissonGenerator(mean)
}

/**
 * This class implements a XORShift random number generator algorithm
 * Source:
 * Marsaglia, G. (2003). Xorshift RNGs. Journal of Statistical Software, Vol. 8, Issue 14.
 * @see <a href="http://www.jstatsoft.org/v08/i14/paper">Paper</a>
 * This implementation is approximately 3.5 times faster than
 * {@link java.util.Random java.util.Random}, partly because of the algorithm, but also due
 * to renouncing thread safety. JDK's implementation uses an AtomicLong seed, this class
 * uses a regular Long. We can forgo thread safety since we use a new instance of the RNG
 * for each thread.
 */
private[marlin] class XORShiftRandom(init: Long) extends Random(init) {

  def this() = this(System.nanoTime)

  private var seed = MTUtils.hashSeed(init)

  // we need to just override next - this will be called by nextInt, nextDouble,
  // nextGaussian, nextLong, etc.
  override protected def next(bits: Int): Int = {
    var nextSeed = seed ^ (seed << 21)
    nextSeed ^= (nextSeed >>> 35)
    nextSeed ^= (nextSeed << 4)
    seed = nextSeed
    (nextSeed & ((1L << bits) -1)).asInstanceOf[Int]
  }

  override def setSeed(s: Long) {
    seed = MTUtils.hashSeed(s)
  }
}
