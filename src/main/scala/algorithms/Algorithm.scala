package algorithms

case class Context(edgeFile: String, nScan: Int)

abstract class Algorithm[Value: Manifest](context: Context, default: Value)
    extends helper.Logging {
  import scala.collection.mutable.{ BitSet, Map }
  import graph.EdgeFile
  import helper.HugeContainers.{ FlatArray, HugeArrayOps }

  val Context(edgeFile, nScan) = context
  private val eFile = EdgeFile(edgeFile)
  def getEdges = eFile.get

  val data = FlatArray[Value](default)
  private var stepCounter = 0
  private val flags = Array.fill(2)(new BitSet)
  private def sFlag = flags(stepCounter & 1)
  private def gFlag = flags((stepCounter + 1) & 1)
  def scatter(id: Long, value: Value) = { val i = id.toInt; sFlag.add(i); data(id, value) }
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

//object Algorithm {
//  import scala.collection.JavaConversions._
//  import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
//  import graph._
//  import helper.Logging
//
//  val system = ActorSystem("SAGE")
//
//  sealed abstract class Messages
//  case class SCAN() extends Messages
//  case class HALT() extends Messages
//
//  class Scanner[T](g: Graph[T], step: Int) extends Actor with Logging {
//    def receive = {
//      case SCAN =>
//        logger.info("scan [{}]", step)
//        sys.exit
//      case _ => logger.error("unidentified message")
//    }
//  }
//
//  val scannerId = (1 to Int.MaxValue).toIterator
//  def scannerName(id: Int) = "scan%08x".format(id)
//  def scannerLaunch[T](g: Graph[T]) = {
//    val id = scannerId.next()
//    system.actorOf(Props(new Scanner(g, id)), name = scannerName(id))
//  }
//
//  class Collector[T](g: Graph[T]) extends Actor with Logging {
//    def receive = {
//      case HALT =>
//        logger.info("Collector stand down"); sys.exit
//      case _ => logger.error("unidentified message")
//    }
//  }
//
//  def collectorLaunch[T](g: Graph[T]) =
//    system.actorOf(Props(new Collector(g)), name = "collector")
//
//  abstract class AlgorithmBase[T](g: Graph[T]) {
//  }
//}