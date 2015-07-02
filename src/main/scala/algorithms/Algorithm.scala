package algorithms

abstract class Algorithm[Value: Manifest] extends helper.Logging {
  import scala.collection.mutable.BitSet
  import scala.collection.concurrent.TrieMap

  val data = TrieMap[Long, Value]()
  private var stepCounter = 0
  private val flags = Array.fill(2)(new BitSet)
  private def sFlag = flags(stepCounter & 1)
  private def gFlag = flags((stepCounter + 1) & 1)
  def scatter(id: Long, value: Value) = { sFlag.add(id.toInt); data(id) = value }
  def scatter = sFlag.toIterator.map(_.toLong)
  def gather(id: Long) = gFlag.contains(id.toInt)
  def gather = gFlag.toIterator.map(_.toLong)
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
    data.toIterator
  }
}
