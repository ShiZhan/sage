/**
 * Gauge operations with console feedback
 */
package helper

/**
 * @author ShiZhan
 * Sequential operations with console feedback
 * E.g.: to process a large amount of files
 */
object Gauge {
  implicit class IteratorOperations[T](items: Iterator[T]) {
    import scala.compat.Platform.currentTime

    val h = 7000L // 7s high
    val l = 2000L // 2s low

    def foreachDo(op: T => Any) = {
      var i = 0L // counter
      var d = 0x80L // counter delta value for print
      var t0 = currentTime

      for (item <- items) {
        op(item)
        i += 1
        if (i % d == 0) {
          print(s"[$i] items processed\r")
          val t1 = currentTime
          val t = t1 - t0
          if (t < l) d << 1 else if (t > h && d > 1) d >> 1
          t0 = t1
        }
      }
      println(s"[$i] items processed")
    }
  }
}