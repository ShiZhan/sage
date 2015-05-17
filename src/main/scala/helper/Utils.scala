package helper

object Utils {
  def isPowerOf2(i: Int) = ((i - 1) & i) == 0
  def getPowerOf2OrElse(i: Int, d: Int) = if (isPowerOf2(i: Int)) i else d 
}
