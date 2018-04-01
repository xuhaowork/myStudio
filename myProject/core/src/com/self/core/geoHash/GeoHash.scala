package src.com.self.core.geoHash

/**
  * --------
  * 备注：
  * --------
  * 实现GeoHash算法
  * 实现：编码到二进制数、编码到字符串、从二进制数解码、从字符串解码方法
  */
class GeoHash(val hashLength: Int = 9) extends Serializable {
  /** Base32编码 */
  val digits: Array[Char] = Array(
    '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
    'b', 'c', 'd', 'e', 'f', 'g', 'h', 'j', 'k', 'm',
    'n', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x',
    'y', 'z')

  /**
    * 经纬度范围
    */
  val longitudeRange: (Double, Double) = (-180.0, 180.0)
  val latitudeRange: (Double, Double) = (-90.0, 90.0)

  /** 单点编码 */
  def encode(longitude: Double, latitude: Double): String = {
    var i = 0
    var flag = true // true时对纬度二分, 否则对经度二分, 以经度二分开头
    var (lgLower: Double, lgUpper: Double) = longitudeRange
    var (laLower: Double, laUpper: Double) = latitudeRange
    var bitNum = 0
    var hashString = ""
    while(i < hashLength * 5){
      if(flag){
        val lgMid: Double = (lgLower + lgUpper) / 2.0
        if(longitude >= lgMid){
          lgLower = lgMid
          bitNum = bitNum * 2 + 1
        }else{
          lgUpper = lgMid
          bitNum = bitNum * 2
        }
      }else{
        val laMid: Double = (laLower + laUpper) / 2
        if(latitude >= laMid){
          laLower = laMid
          bitNum = bitNum * 2 + 1
        }else{
          laUpper = laMid
          bitNum = bitNum * 2
        }
      }
      if(i % 5 == 4){
        hashString += digits(bitNum)
        bitNum = 0
      }
      flag = !flag
      i += 1
    }
    hashString
  }


  /** 返回的是Array形式, 方便九点编码获取周围的八个点 */
  def encodeByArr(longitude: Double, latitude: Double): String = {
    var i = 0
    var flag = true // true时对纬度二分, 否则对经度二分, 以经度二分开头
    var (lgLower: Double, lgUpper: Double) = longitudeRange
    var (laLower: Double, laUpper: Double) = latitudeRange
    var bitNum = 0
    var hashString = ""
    while(i < hashLength * 5){
      if(flag){
        val lgMid: Double = (lgLower + lgUpper) / 2.0
        if(longitude >= lgMid){
          lgLower = lgMid
          bitNum = bitNum * 2 + 1
        }else{
          lgUpper = lgMid
          bitNum = bitNum * 2
        }
      }else{
        val laMid: Double = (laLower + laUpper) / 2
        if(latitude >= laMid){
          laLower = laMid
          bitNum = bitNum * 2 + 1
        }else{
          laUpper = laMid
          bitNum = bitNum * 2
        }
      }
      if(i % 5 == 4){
        hashString += digits(bitNum)
        bitNum = 0
      }
      flag = !flag
      i += 1
    }
    hashString
  }









}


