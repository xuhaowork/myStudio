package src.com.self.core.geoHash

/**
  * --------
  * 备注：
  * --------
  * 实现GeoHash算法
  * 实现：编码到二进制数、编码到字符串、从二进制数解码、从字符串解码方法
  */
class GeoHash(val hashLength: Int = 9) extends Serializable {
  /** 经纬度的迭代次数 */
  val iterNumsInfo = GeoIterNumsInfo(hashLength * 5 - hashLength / 2 *5, hashLength / 2 *5)

  /** Base32编码 */
  var digits = Array('0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm', 'n', 'p', 'q',
    'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z')

  /**
    * 经度初始迭代 --用于编码位数为奇数的情况, 此时经度需要先迭代一次
    * ----
    */
  private def initial(axis: Double, range: (Double, Double)): (Double, Boolean) = {
    val mid = (range._1 + range._2) / 2
    if(axis >= mid) (axis - mid, true) else (mid - axis, false)
  }

  /** 迭代 */
  def encode(longitude: Double, latitude: Double): String = {
    var flag = true // true时对纬度二分, 否则对经度二分, 以经度二分开头
    var (lgLower: Double, lgUpper: Double) = (-180.0, 180.0)
    var (laLower: Double, laUpper: Double) = (-90.0, 90.0)
    var bitNum = 0
    val hashString = new Array[Int](hashLength)
    for(i <- 0 until (hashLength * 5)){
      if(flag){
        val lgMid: Double = (lgLower + lgUpper) / 2.0
        if(longitude >= lgMid){
          lgLower = lgMid
          bitNum = bitNum * 2 + 1
        }else{
          lgUpper = lgMid
        }
      }else{
        val laMid: Double = (laLower + laUpper) / 2
        if(latitude >= laMid){
          laLower = laMid
          bitNum = bitNum * 2 + 1
        }else{
          laUpper = laMid
        }
      }
      if(i % 5 == 4){
        hashString(i / 5) = digits(bitNum)
        bitNum = 0
      }
      flag = !flag
    }
    hashString.mkString("")
  }







}


