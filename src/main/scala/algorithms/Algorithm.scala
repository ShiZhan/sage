package algorithms

case class Context(edgeFile: String, nScan: Int)

abstract class Algorithm[Value: Manifest](context: Context, default: Value)
    extends helper.Logging {
  import scala.collection.mutable.BitSet
  import graph.EdgeFile
  import helper.HugeContainers._

  val Context(edgeFile, nScan) = context
  private val eFile = EdgeFile(edgeFile)
  def getEdges = eFile.get

  val data = GrowingArray[Value](default)
  private var stepCounter = 0
  private val flags = Array.fill(2)(new BitSet)
  private def sFlag = flags(stepCounter & 1)
  private def gFlag = flags((stepCounter + 1) & 1)
  def scatter(id: Long, value: Value) = { val i = id.toInt; sFlag.add(i); data(id) = value }
  def scatter = sFlag.toIterator.map(_.toLong)
  def gather(id: Long) = gFlag.contains(id.toInt)
  def gather = !gFlag.isEmpty
  def update = {
    val stat = "[ % 10d -> % 10d ]".format(gFlag.size, sFlag.size)
    gFlag.clear()
    stepCounter += 1
    logger.info("Step {}: {}", stepCounter, stat)
  }

  def iterations: Unit

  def run = {
    logger.info("Edge List: [{}]", eFile.p.getFileName)
    logger.info("Threads:   [{}]", nScan)
    iterations
    eFile.close
    if (data.used == 0)
      None
    else {
      logger.info("Generating results ...")
      val result = data.inUse
      Some(result)
    }
  }
}
