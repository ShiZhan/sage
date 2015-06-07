/**
 * JVM states
 */
package helper

/**
 * @author ShiZhan
 * Get JVM states information
 */
object JStates {
  private val runtime = Runtime.getRuntime

  object Memory {
    def free = runtime.freeMemory
    def total = runtime.totalMemory
    def used = total - free
    def max = runtime.maxMemory
  }
}