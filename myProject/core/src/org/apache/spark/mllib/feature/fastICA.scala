package org.apache.spark.mllib.feature

import breeze.linalg
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.mllib.feature.VectorTransformer
import org.apache.spark.mllib.linalg.{DenseMatrix, EigenValueDecomposition, SparseMatrix, Vector, Vectors}
import org.apache.spark.mllib.linalg.distributed.RowMatrix
import org.apache.spark.mllib.linalg.Matrix
import org.apache.spark.sql.DataFrame
import breeze.linalg.{diag, eig, DenseMatrix => BDM, DenseVector => BDV, svd => brzSvd}
import breeze.optimize.MaxIterations
import org.apache.spark.mllib.feature.PCA
import org.apache.spark.rdd.RDD

import scala.math
import scala.collection.mutable



/**
  * An algorithm for Independent Component Analysis.
  * Which can be refer to :
  * [1]'A. Hyvarinen and E. Oja (2000) Independent Component
  * Analysis: Algorithms and Applications, Neural Networks, 13(4-5):411-430'
  * [2]'r/CRAN/web/packages/fastICA/fastICA.pdf'
  */


/** Generally speaking:
  * We assume that the data is X with dims of N*n,what we want is to find a
  * matrix A and S with dims of N*m, A*S = X. The latent data of S is component
--------------------------------------------------------------------------------
  * of n variables which is nonGaussian.
  *
  * First we need to white X to Z, assume that E(x*t(x)) = E*D*E, then
  * Z = `E {D}-1 E`.
  *
  * Second, we need to find a matrix A to transform Z to S which is the most
  * nonGaussian:
  *   1)The way to identify the optimum solution is projection pursue in which
  *   the nonGaussian is measured by a statistic -- neg-entropy;
  *   2)We approximate the neg-entropy by `E{{g(w'*x) - gaussian}^2}`;
  *   3)Under Kuhn-Tucker condition we can locate the optimum solution by fixed
  *   point -- E{xg(w'*x)} - beta*W = 0, beta = E{w*'Xg(w*'x)}, w* is the
  *   optimum solution.
  *   4)We can use Newton algorithm to locate the root of the equation --
  *   w := w - f(x)/f'(x)
  *   f'(x) = E{x*x'g'(w'*x)} - beta, approximate to E{g'(w'*x)} - beta
  *   we estimate the beta via assume w* is w in this iteration.
  *   5) So the final iteration equation is:
  *   w := E{xg(w'*x)} - E{g'(w'*x)}*w;
  *   w = w / ||w||.
  */
--------------------------------------------------------------------------------
class fastICA(private var componetNums: Int, private var runs: Int,
              private var methods: String, private var whiteMatrixByPCA:
              (Boolean, Int)){
  def this() = this(3, 1, "symmetric", (false , 3))

  val fraction = 0.8
  val seed = 123L

  /** The independent component numbers. */
  def setComponetNums(componetNums: Int): this.type = {
    this.componetNums = componetNums
    this
  }

  /**
    * The runs in each time, we choose a better one from the runs to avoid the
    * issues of initialization.
    */
  def setRuns(runs: Int): this.type = {
    this.runs = runs
    this
  }

  def setMethods(methods: String): this.type = {
    this.methods = methods
    this
  }

  def setWhiteMatrixByPCA(num: Int): this.type = {
    this.whiteMatrixByPCA = (true, num)
    this
  }

  private def getSuperNums(colNums: Int): Int = {
    if(this.whiteMatrixByPCA._1){
      val PCANums = this.whiteMatrixByPCA._2
      require(colNums >= PCANums && PCANums >= this.componetNums,
        "The numbers of columns and PCA components should not be less than " +
          "componentNums.")
      PCANums
    } else
      this.componetNums
  }


  private def checkNums(colNums: Long, rowNums: Long): Boolean =
    componetNums <= scala.math.min(colNums, rowNums)

  /** Transform the matrix to a matrix with zero means. Used in whiteMatrix. */
  private def standardWithMean(matrix: RowMatrix)
  : RowMatrix = {
    val mean = matrix.computeColumnSummaryStatistics().mean.toArray
    val meanBC = matrix.rows.context.broadcast(mean)
    val result = matrix.rows.mapPartitions(iter =>
      iter.map(v => {
        val bv = v.toArray.zip(meanBC.value)
          .map{case (e, means) => e - means}
        Vectors.dense(bv)
      })
    )
    new RowMatrix(result)
  }


  /** Transform the matrix with PCA. Used in whiteMatrix. */
  def computePrincipalModel(matrix: RowMatrix, k: Int): PCAModel = {
    val pc = matrix.computePrincipalComponents(k) match {
      case dm: DenseMatrix =>
        dm
      case sm: SparseMatrix =>
        // In the step of transform a matix to a principal matrix, the
        // sparse vector should be transformed to a dense vector.
//------------------------------------------------------------------------------
        sm.toDense
      case m =>
        throw new IllegalArgumentException("Unsupported matrix format. " +
          s"Expected SparseMatrix or DenseMatrix. Instead got: ${m.getClass}")
    }
    new PCAModel(k, pc)
  }



  private def orthogonalize(matrix: RowMatrix, colNums: Long, rowNums: Long)
  : RowMatrix = {
    val svdMatrix = matrix.computeSVD(colNums.toInt)
    val checkZero = svdMatrix.s.toBreeze.min
    require(checkZero > 0, "The rank of the matrix is less than the " +
      "component nums you want, if you want the result of the data you " +
      "should try PCA in the white stage.")

    val sqrtEigen = svdMatrix.s.toBreeze
      .map(eigen => 1 / scala.math.sqrt(eigen))

    val sqrtEigenMatrix = DenseMatrix.diag(Vectors.fromBreeze(sqrtEigen))

    svdMatrix.U.multiply(sqrtEigenMatrix).multiply(svdMatrix.V.transpose)
  }


  private def orthogonalize(matrix: BDM[Double])
  : (BDV[Double], BDM[Double]) = {
    val eigObj = eig.apply(matrix)
    (eigObj.eigenvalues, eigObj.eigenvectors)
  }




  /** Transform the matrix to a white matrix. */
  private def whiteMatrix(matrix: RowMatrix, colNums: Long, rowNums: Long) = {
    val meanScaleMatrix = standardWithMean(matrix)
    val svdNums = getSuperNums(colNums.toInt)
    val principalModel = computePrincipalModel(meanScaleMatrix, svdNums)
    val whiteMatrix = principalModel.pc

    val whiteFeatures = principalModel.transform(matrix.rows)
    (whiteFeatures, whiteMatrix)
  }

  def fit(matrix: RowMatrix): ICAmodel = {

    val colNums = matrix.numCols()
    val rowNums = matrix.numRows()
    require(checkNums(colNums, rowNums), "The num of components " +
      "should be not bigger than colNums and rowNums.The max number here" +
      s" is ${math.min(colNums, rowNums)}")

    val tol = 1e-10
    val whiteModel = whiteMatrix(matrix: RowMatrix, colNums: Long, rowNums: Long)
    val whiteFeatures: RDD[Vector] = whiteModel._1


    val W: BDM[Double] = initialWeight(colNums)


    projectionPursuit(whiteFeatures)


  }



  private def initialWeight(pColNums: Int): Matrix = {
    val rd = new util.Random()
    val weightArr = Array
      .tabulate(componetNums*pColNums)(_ => rd.nextDouble())

    new DenseMatrix(pColNums, componetNums, weightArr)
  }



  private def subtract(x1: DenseMatrix, x2: DenseMatrix): DenseMatrix = {
    new DenseMatrix(x1.numCols, x1.numRows, x1.values.zip(x2.values).map{
      case (d1, d2) => d1 - d2
    })
  }


  private def projectionPursuit(matrix: RDD[Vector],
                                initialWeight: Matrix,
                                pColNums: Int, rowNums: Int) = {
    var W_old = initialWeight.copy
    var W = initialWeight.copy
    val x = new RowMatrix(matrix)

    var xw = x.multiply(W)
    val wGwX = xw.rows.map(x => {
      val gwx = x.toArray.map(e => e*math.tanh(e))
      Vectors.dense(x.toArray.flatMap(d => gwx.map(_*d)))
    }).sample(false, fraction, seed)

    val E_Gx_Arr = new RowMatrix(wGwX).computeColumnSummaryStatistics()
      .mean.toArray

    val E_Gx = new DenseMatrix(componetNums, pColNums, E_Gx_Arr)

    val E_Gdx = xw.rows.map(v => {
      val u = v.toArray.map(d =>
      1 - math.tanh(d)*math.tanh(d))
      Vectors.dense(u)
    }).sample(false, fraction, seed)

    val gdx_arr: BDV[Double] = new RowMatrix(E_Gdx)
      .computeColumnSummaryStatistics().mean.toBreeze.toDenseVector
    val diag_gdx = new DenseMatrix(componetNums, pColNums, diag(gdx_arr).data)


    val s1 = W.multiply(diag_gdx)
    val s2 = E_Gx

    val s = subtract(E_Gx, s1).toBreeze.asInstanceOf[BDM[Double]]
    orthogonalize(s)._2.toDenseMatrix
    W = new DenseMatrix(orthogonalize(s)._2.cols, orthogonalize(s)._2.rows, orthogonalize(s)._2.data)

    // 当W方向收敛时认为收敛
    W_old.
    val distance = W.multiply()

  }





}


class ICAmodel extends VectorTransformer {
  val A = 0
  override def transform(vector: Vector)
}
