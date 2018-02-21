package org.apache.spark.mllib.clustering

import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.rdd.RDD
//import org.apache.spark.util.Utils
import org.apache.spark.mllib.clustering.KMeansModel
import org.apache.spark.storage.StorageLevel
import org.apache.spark.internal.Logging
import org.apache.spark.util.random.XORShiftRandom
import org.apache.spark.mllib.linalg.BLAS.{axpy, scal}
import scala.collection.mutable.{Map => mutableMap}


//-------------------------------------------------------------------

class ElkKmeans private(private var k: Int,
                        private var maxIterations: Int,
                        private var initializationMode: String,
                        private var epsilon: Double,
                        private var seed: Long)
  extends Serializable with Logging{
  def this() = this(2, 200, "random", 1e-2, (new util.Random).nextInt(1))

  /** The number of the clusters */
  def setK(k: Int): this.type = {
    this.k = k
    this
  }

  /** The max number for iterations */
  def setMaxIterations(maxIterations: Int): this.type = {
    this.maxIterations = maxIterations
    this
  }

  /**
    * The threshold distance for the algorithm to converge
    * between old centers and new ones
    */
  def setEpsilon(epsilon: Double): this.type = {
    this.epsilon = epsilon
    this
  }

  /** The random seed */
  def setSeed(seed: Long): this.type = {
    this.seed = seed
    this
  }

  /** The initialModel maybe we use. */
  private var initialModel: Option[KMeansModel] = None

  /**
    * Maybe we can use a initial mode that trained before rather
    * than random or kmeans||
    */
  def setInitialModel(model: KMeansModel): this.type = {
    require(model.k == k, "mismatched cluster count")
    initialModel = Some(model)
    this
  }

//-------------------------------------------------------------------

  /** The main run function. */
  def run(data: RDD[Vector]): KMeansModel = {
    if (data.getStorageLevel == StorageLevel.NONE) {
      logWarning("The input data is not directly cached, which may" +
        " hurt performance if its parent RDDs are also uncached.")
    }

    // initial centers
    val initialCenters: KCenters = initCenters(data)

    // first iteration
    // ----compute the center and bound information for each point.
    val (dataWithBound, inputCenters) =
    firstIteration(data, initialCenters)

    println("first iteration: ")
    inputCenters.centers.foreach(println)

    // Lloyd iteration
    Lloyd(dataWithBound, inputCenters)
  }

//-------------------------------------------------------------------
  /** The Lloyd iteration function. */
  private def Lloyd(data: RDD[VectorWithBound],
                    inputCenters: KCenters)
  : KMeansModel = {
    val sc = data.sparkContext
    var centers = inputCenters
    val centersBC = sc.broadcast(centers)
    var i = 0
    var flag = true
    while (i < maxIterations && flag) {
      // Find closest centers and bound information.
      var iteratorRdd = data.map{
        vec => findClosest(vec, centersBC.value)
      }

//-------------------------------------------------------------------
      // Update the centers.
      val centerArr = iteratorRdd
        .map(vec => (vec.id, (1, vec.vector)))
        .reduceByKey {
        case ((c1, v1), (c2, v2)) =>
          axpy(1.0, v1, v2)
          (c1 + c2, v2)
        }.collect()
        .map {
        case (key, (count, sv)) =>
          scal(1.0 / count, sv)
          (key, sv)
        }.sortBy(_._1).map(_._2)

      // Compute the move of each centers
      val move = centerArr.zip(centers.centers)
        .map{
          case (v1, v2) => Vectors.sqdist(v1, v2)
        }

      // Whether algorithm converge.
      flag = !move.forall(mv => mv < epsilon*epsilon)

      // Update low, upper and r.
      iteratorRdd = iteratorRdd.map(vec => {
        var low = vec.low
        low = low.zip(move)
          .map{
            case (before, mv) => math.max(before - mv, 0.0)
          }

        var upper = vec.up
        upper = upper + move(vec.id)

        VectorWithBound(vec.vector, vec.id, low, upper, true)
      })


      // Update the centers.
      centers = new KCenters(centerArr)
      i += 1
    }
    println(s"iteration steps : $i")

    // Output.
    new KMeansModel(centers.centers)
  }


  /** The function initialize centers */
  private def initCenters(data: RDD[Vector])
  : KCenters = {
    initialModel match {
      case Some(kMeansCenters) => {
        new KCenters(kMeansCenters.clusterCenters)
      }
      case None => {
        if (initializationMode == "random") {
          new KCenters(initRandom(data))
        }else{
          throw new Exception("initializationMode must be random" +
            " now when initialModel does not exist.")
        }
      }
    }
  }

//-------------------------------------------------------------------
  private def initRandom(data: RDD[Vector]): Array[Vector] =
    data.takeSample(false, k, new XORShiftRandom(seed).nextInt())
      .toSeq.toArray



  /** Find closest centers */
  private def findClosest(points: Vector, Centers: KCenters)
  : (Int, Double) = {
    val inferCost = Centers.DisMap.values.min
    var bestId = 0
    var bestCost = Double.PositiveInfinity
    var i = 0
    var flag = true

    while(flag && i < Centers.centers.length){
      val cost = Vectors.sqdist(Centers.centers(i), points)
      if(cost < bestCost){
        bestId = i
        bestCost = cost
      }
      if(bestCost < inferCost / 2)
        flag = false
      i += 1
    }
    (bestId, bestCost)
  }
//-------------------------------------------------------------------
  /** Find closest centers when we have more information of bound */
  private def findClosest(points: VectorWithBound, centers: KCenters)
  : VectorWithBound = {
    // --------------------------------------------
    val minCost: Double = centers.minCost
    val v: Vector = points.vector
    var id: Int = points.id
    val low: Array[Double] = points.low
    var upper: Double = points.up
    var r: Boolean = points.r
//-------------------------------------------------------------------
    if (upper < minCost) {
      val restCenters = Array
        .tabulate(k - 1)(i => if (i < id) i else i + 1)
      restCenters.foreach {
        i => {
          val centerI = centers.centers(i)
          if(upper > low(i) && upper > centers.disCompute(i, id)/2){
            var dist2center = upper
            if (r) {
              dist2center = Vectors.sqdist(centers.centers(id), v)
              low(id) = dist2center
              upper = dist2center
              r = false
            }
            if(dist2center > low(i) || dist2center > centers.disCompute(i, id)/2) {
              val dis = Vectors.sqdist(centerI, v)
//-------------------------------------------------------------------
              if(dist2center <
                Vectors.sqdist(centers.centers(id), v)) { // vectorWithBound need to add a cost.
                id = i
                upper = dis
                low(id) = dis
              }
            }
          }
        }
      }
    }

    VectorWithBound(v, id, low, upper, r)
  }
//-------------------------------------------------------------------

  /**
    * Fist iteration, in first iteration we need to compute the
    * closest center each point belong to and the bound information.
    */

  private def firstIteration(data: RDD[Vector],
                             initialCenter: KCenters)
  : (RDD[VectorWithBound], KCenters) = {
    // Update data with the closest centers and bound information.
    val sc = data.sparkContext
    val bcCenters = sc.broadcast(initialCenter)
    val dataWithBound = data.map{point =>
      val (id, cost) = findClosest(point, bcCenters.value)
      val low = Array.tabulate(k)(i => if(i == id) cost else 0.0)
      val up = cost
      VectorWithBound(point, id, low, up, false)
    }

    // Update the centers.
    val centersArr = dataWithBound.map(v => (v.id, (1, v.vector)))
      .reduceByKey{
      case ((c1, v1), (c2, v2)) =>
        axpy(1.0, v1, v2)
        (c1 + c2, v2)
    }.collect().map{
        case (key, (count, sv)) =>
          scal(1.0 / count, sv)
          (key, sv)
      }.sortBy(_._1).map(_._2)

    centersArr.foreach(println)
    val newCenters = new KCenters(centersArr)
    (dataWithBound, newCenters)
  }



}

object ElkKmeans{
  def printk(): Unit = {
    println(new ElkKmeans().k)
  }


}



//-------------------------------------------------------------------

case class VectorWithBound(vector: Vector,
                           id: Int,
                           low: Array[Double],
                           up: Double,
                           r: Boolean)

class KCenters(val centers: Array[Vector]) extends Serializable {
  val DisMap: mutableMap[(Int, Int), Double] = {
    val muMap = mutableMap.empty[(Int, Int), Double]
    for(i <- centers.indices){
      for(j <- 0 until i){
        muMap += (i, j) -> Vectors.sqdist(centers(i), centers(j))
      }
    }
    muMap
  }

  val minCost: Double = DisMap.values.min

  def disCompute(i: Int, j: Int): Double =
    if(i < j) DisMap(j, i) else DisMap(i, j)
}