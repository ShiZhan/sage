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

  object MEM {
    def FREE = runtime.freeMemory
    def TOTAL = runtime.totalMemory
    def USED = TOTAL - FREE
    def MAX = runtime.maxMemory
  }
}