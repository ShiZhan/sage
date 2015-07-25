package algorithms.parallel

abstract class Algorithm[Value: Manifest](default: Value) extends helper.Logging {
  import scala.collection.mutable.BitSet
  import helper.GrowingArray

  val vertices = GrowingArray[Value](default)
  private var stepCounter = 0
  private val flags = Array.fill(2)(new BitSet)
  def scatter = flags(stepCounter & 1)
  def gather = flags((stepCounter + 1) & 1)
  def scatter(id: Int, value: Value): Unit = { scatter.add(id); vertices(id) = value }
  def update() = {
    val stat = "[ % 10d -> % 10d ]".format(scatter.size, gather.size)
    gather.clear()
    stepCounter += 1
    logger.info("Step {}: {}", stepCounter, stat)
  }

  def iterations(): Unit

  def run = {
    logger.info("Start ...")
    iterations
    vertices.updated
  }
}
