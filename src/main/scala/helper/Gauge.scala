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
    def foreachDo(op: T => Any) = {
      var i = 0L
      var delta = 0x10L
      for (item <- items) {
        op(item)
        i += 1
        if (i % delta == 0) print(s"[$i] items processed\r")
        delta = if (i < 0x100) 1 else if (i < 0x10000) 0x100 else if (i < 0x1000000) 0x10000 else 0x1000000
      }
      println(s"[$i] items processed")
    }
  }
}