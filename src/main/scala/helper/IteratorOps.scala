package helper

/**
 * @author ShiZhan
 * Iterator Modifiers
 * VisualOperations:        Sequential operations with console feedback
 * ClosableIteratorWrapper: Iterator with a close function
 */
object IteratorOps {
  implicit class VisualOperations[T](elems: Iterator[T]) {
    import scala.compat.Platform.currentTime

    val h = 7000L // 7s high
    val l = 2000L // 2s low

    def foreachDo(op: T => Any) = {
      var i = 0L // counter
      var d = 0x80L // counter delta value for print
      var t0 = currentTime

      for (elem <- elems) {
        op(elem)
        i += 1
        if ((i & (d - 1)) == 0) {
          print(s"[$i]\r")
          val t1 = currentTime
          val t = t1 - t0
          if (t < l) d <<= 1 else if (t > h && d > 1) d >>= 1
          t0 = t1
        }
      }
      println(s"[$i]")
    }

    def foreachDoWithScale(scale: Int)(op: T => Any) = {
      var i = 0L
      var d: Long = 1 << 7
      var t0 = currentTime
      val s = 1 << scale

      for (elem <- elems) {
        op(elem)
        i += s
        if ((i & (d - 1)) == 0) {
          print(s"[$i]\r")
          val t1 = currentTime
          val t = t1 - t0
          if (t < l) d <<= 1 else if (t > h && d > 1) d >>= 1
          t0 = t1
        }
      }
      println(s"[$i]")
    }
  }

  implicit class ClosableIteratorWrapper[T](elems: Iterator[T]) {
    def toClosableIterator(close: () => Unit) =
      Iterator.continually { elems.next }
        .takeWhile { _ => if (elems.hasNext) true else { close(); false } }
  }
}