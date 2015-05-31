package algorithms

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

case class Context(edgeFile: String, vdbFile: String, nScan: Int)

abstract class Algorithm[Value](context: Context) extends helper.Logging {
  import scala.collection.JavaConversions._
  import java.io.File
  import java.util.concurrent.ConcurrentNavigableMap
  import org.mapdb.DBMaker
  import graph.EdgeFile
  type Vertices = ConcurrentNavigableMap[Long, Value]
  val Context(edgeFile, vdbFile, nScan) = context
  val E = EdgeFile(edgeFile)
  val file = if (vdbFile.isEmpty()) configuration.Options.getCache else new File(vdbFile)
  val db = DBMaker.newFileDB(file).closeOnJvmShutdown().make()

  var stepCounter = 0
  def step(i: Int): Vertices = db.getTreeMap(s"$i")
  val data: Vertices = db.getTreeMap("data")
  def gather = step(stepCounter)
  def scatter = step(stepCounter + 1)
  def update = {
    val stat = "[ % 10d -> % 10d ]".format(gather.size, scatter.size)
    gather.clear()
    data.putAll(scatter)
    db.commit()
    stepCounter += 1
    logger.info("Step {}: {}", stepCounter, stat)
  }

  def iterations: Unit

  def run = {
    logger.info("Edge List: [{}]", edgeFile)
    logger.info("Threads:   [{}]", nScan)
    logger.info("Vertex DB: [{}]", vdbFile)
    iterations
    E.close
    if (data.isEmpty())
      None
    else {
      logger.info("Generating results ...")
      val result = data.toIterator
      Some(result)
    }
  }
}