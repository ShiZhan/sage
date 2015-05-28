/**
 * Timer
 */
package helper

/**
 * @author ShiZhan
 * Timer
 */
object Timing {
  implicit class TimerWrapper[T](op: () => T) {
    def elapsed = {
      val t1 = compat.Platform.currentTime
      val result = op()
      val t2 = compat.Platform.currentTime
      (result, t2 - t1)
    }
  }

  implicit class TimerWrapperUnit(op: () => Unit) {
    def elapsed = {
      val t1 = compat.Platform.currentTime
      op()
      val t2 = compat.Platform.currentTime
      t2 - t1
    }
  }
}
