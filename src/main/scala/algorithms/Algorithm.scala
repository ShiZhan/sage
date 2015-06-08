package algorithms

case class Context(edgeFile: String, nScan: Int)

abstract class Algorithm[Value: Manifest](context: Context, default: Value)
    extends helper.Logging {
  import scala.collection.mutable.BitSet
  import graph.EdgeFile
  import helper.HugeContainers.{ GrowingArray, HugeArrayOps }

  val Context(edgeFile, nScan) = context
  private val eFile = EdgeFile(edgeFile)
  def getEdges = eFile.get

  val data = GrowingArray[Value](default)
  private var stepCounter = 0
  private val flags = Array.fill(2)(new BitSet)
  private def sFlag = flags(stepCounter & 1)
  private def gFlag = flags((stepCounter + 1) & 1)
  def scatter(id: Long, value: Value) = { val i = id.toInt; sFlag.add(i); data(id) = value }
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

abstract class AlgorithmN[Value: Manifest](context: Context, default: Value) {
  import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
  import graph.{ Edge, EdgeFile }
  import helper.HugeContainers.{ FlatArray, HugeArrayOps }
  import helper.Logging

  val Context(edgeFile, nScan) = context
  private val eFile = EdgeFile(edgeFile)
  val nEdges = eFile.total

  val system = ActorSystem("SAGE")

  sealed abstract class Messages
  case class SCAN() extends Messages
  case class HALT() extends Messages

  val data = FlatArray[Value](default)

  class Scanner[T](edges: Iterator[Edge], step: Int) extends Actor with Logging {
    def receive = {
      case SCAN =>
        logger.info("scan [{}]", step)
        sys.exit
      case _ => logger.error("unidentified message")
    }
  }

  def scannerName(id: Int) = "scan%08x".format(id)

  class Collector extends Actor with Logging {
    def receive = {
      case HALT =>
        logger.info("Collector stand down"); sys.exit
      case _ => logger.error("unidentified message")
    }
  }

  def launch[T](edges: Iterator[Edge]) = {
    (0 to (nScan - 1)).foreach { id => system.actorOf(Props(new Scanner(edges, id)), name = scannerName(id)) }
    system.actorOf(Props(new Collector), name = "collector")
  }
}