//package org.apache.spark.clustering.mllib
//
//import org.apache.spark.mllib.linalg.{Vector, Vectors}
//import org.apache.spark.rdd.RDD
//import org.apache.spark.util.Utils
//import org.apache.spark.mllib.clustering.KMeansModel
//import org.apache.spark.storage.StorageLevel
//import org.apache.spark.Logging
//import org.apache.spark.util.random.XORShiftRandom
//
//import scala.collection.mutable.{Map => mutableMap}
//
//
////-------------------------------------------------------------------
//
//class ElkKmeans private(private var k: Int,
//                        private var maxIterations: Int,
//                        private var initializationMode: String,
//                        private var epsilon: Double,
//                        private var seed: Long)
//  extends Serializable with Logging{
//  def this() = this(2, 20, "kmeans||", 1e-4, Utils.random.nextLong())
//
//  /** The number of the clusters */
//  def setK(k: Int): this.type = {
//    this.k = k
//    this
//  }
//
//  /** The max number for iterations */
//  def setMaxIterations(maxIterations: Int): this.type = {
//    this.maxIterations = maxIterations
//    this
//  }
//
//  /**
//    * The threshold distance for the algorithm to converge
//    * between old centers and new ones
//    */
//  def setEpsilon(epsilon: Double): this.type = {
//    this.epsilon = epsilon
//    this
//  }
//
//  /** The random seed */
//  def setSeed(seed: Long): this.type = {
//    this.seed = seed
//    this
//  }
//
//  /** The initialModel maybe we use. */
//  private var initialModel: Option[KMeansModel] = None
//
//  /**
//    * Maybe we can use a initial mode that trained before rather
//    * than random or kmeans||
//    */
//  def setInitialModel(model: KMeansModel): this.type = {
//    require(model.k == k, "mismatched cluster count")
//    initialModel = Some(model)
//    this
//  }
//
////-------------------------------------------------------------------
//
//  /** The main run function. */
//  def run(data: RDD[Vector]): KMeansModel = {
//    if (data.getStorageLevel == StorageLevel.NONE) {
//      logWarning("The input data is not directly cached, which may" +
//        " hurt performance if its parent RDDs are also uncached.")
//    }
//
//    // initial centers
//    val initialCenters: KCenters = initCenters(data)
//
//    // first iteration
//    // ----compute the center and bound information for each point.
//    val dataWithBound: RDD[VectorWithBound] = firstIteration(data, initialCenters)
//
//    // Lloyd iteration
//    Lloyd(dataWithBound)
//
//
//  }
//
//
//  /** The Lloyd iteration function. */
//  private def Lloyd(data: RDD[VectorWithBound])
//  : KMeansModel = {
//
//
//    //
//
//
//  }
//
//
//  /** The function initialize centers */
//  private def initCenters(data: RDD[Vector])
//  : KCenters = {
//    initialModel match {
//      case Some(kMeansCenters) => {
//        new KCenters(kMeansCenters.clusterCenters)
//      }
//      case None => {
//        if (initializationMode == "random") {
//          new KCenters(initRandom(data))
//        }else{
//          throw new Exception("initializationMode must be random" +
//            " now when initialModel does not exist.")
//        }
//      }
//    }
//  }
//
////-------------------------------------------------------------------
//  private def initRandom(data: RDD[Vector]): Array[Vector] =
//    data.takeSample(false, k, new XORShiftRandom(seed).nextInt())
//      .toSeq.toArray
//
//
//
//  /** Find closest centers */
//  private def findClosest(points: Vector): (Int, Double) = {
//
//  }
//
//  /** Find closest centers when we have more information of bound */
//  private def findClosest(points: VectorWithBound): (Int, Double) = {
//
//
//  }
////-------------------------------------------------------------------
//
//  /**
//    * Fist iteration, in first iteration we need to compute the
//    * closest center each point belong to and the bound information.
//    */
//
//  private def firstIteration(data: RDD[Vector],
//                             initialCenter: KCenters)
//  : RDD[VectorWithBound] = {
//
//
//
//
//
//
//  }
//
//
//
//}
//
//object ElkKmeans{
//  def printk(): Unit = {
//    println(new ElkKmeans().k)
//  }
//
//
//}
//
//
//
////-------------------------------------------------------------------
//
//case class VectorWithBound(vector: Vector,
//                           id: Int,
//                           l: Array[Vector],
//                           u: Double,
//                           r: Boolean)
//
//class KCenters(val centers: Array[Vector]) extends Serializable {
//  val DisMap = {
//    val muMap = mutableMap.empty[(Int, Int), Double]
//    for(i <- centers.indices){
//      for(j <- 0 until i){
//        muMap += (i, j) -> Vectors.sqdist(centers(i), centers(j))
//      }
//    }
//    muMap
//  }
//
//  def disCompute(i: Int, j: Int): Double =
//    if(i < j) DisMap(j, i) else DisMap(i, j)
//}