package algorithms

abstract class Algorithm[Value: Manifest](default: Value) extends helper.Logging {
  import scala.collection.mutable.BitSet
  import helper.GrowingArray

  val vertices = GrowingArray[Value](default)
  private var stepCounter = 0
  private val flags = Array.fill(2)(new BitSet)
  private def sFlag = flags(stepCounter & 1)
  private def gFlag = flags((stepCounter + 1) & 1)
  def scatter(id: Int, value: Value) = { sFlag.add(id); vertices(id) = value }
  def scatter = sFlag.iterator
  def gather(id: Int) = gFlag.contains(id)
  def gather = gFlag.iterator
  def update() = {
    val stat = "[ % 10d -> % 10d ]".format(gFlag.size, sFlag.size)
    gFlag.clear()
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
