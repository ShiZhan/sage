package algorithms

object Algorithm {
  import scala.collection.JavaConversions._
  import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
  import graph._
  import helper.Logging

  val system = ActorSystem("SAGE")

  sealed abstract class Messages
  case class SCAN() extends Messages
  case class HALT() extends Messages

  class Scanner[T](g: Graph[T], step: Int) extends Actor with Logging {
    def receive = {
      case SCAN =>
        logger.info("scan [{}]", step)
        sys.exit
      case _ => logger.error("unidentified message")
    }
  }

  val scannerId = (1 to Int.MaxValue).toIterator
  def scannerName(id: Int) = "scan%08x".format(id)
  def scannerLaunch[T](g: Graph[T]) = {
    val id = scannerId.next()
    system.actorOf(Props(new Scanner(g, id)), name = scannerName(id))
  }

  class Collector[T](g: Graph[T]) extends Actor with Logging {
    def receive = {
      case HALT =>
        logger.info("Collector stand down"); sys.exit
      case _ => logger.error("unidentified message")
    }
  }

  def collectorLaunch[T](g: Graph[T]) =
    system.actorOf(Props(new Collector(g)), name = "collector")

  abstract class AlgorithmBase[T](g: Graph[T]) {
  }
}

//abstract class Algorithm[Value](context: Context)
//    extends helper.Logging {
//  val Context(prefix, nShard, verticesDB) = context
//  val shards: Shards
//  val vdb: Vertices[Value]
//
//  val system = ActorSystem("CoreSystem")
//  val followers = shards.getAllShards { index =>
//    system.actorOf(Props(new Follower(index)), name = "follower" + "%03d".format(index))
//  }
//
//  var stepCounter = 0
//  def step(i: Int) = vdb.getVertexTable(s"$i")
//  val data = step(0)
//  def gather = step(stepCounter)
//  def scatter = step(stepCounter + 1)
//  def update = {
//    val flags = shards match { case s: DirectionalShards => s.getFlagState; case _ => "" }
//    val stat = "[ % 10d -> % 10d ] %s".format(gather.size, scatter.size, flags)
//    gather.clear()
//    data.putAll(scatter)
//    stepCounter += 1
//    logger.info("Step {}: {}", stepCounter, stat)
//  }
//
//  def iterations: Unit
//
//  def run =
//    if (shards.intact) {
//      logger.info("Data:      [{}]", prefix)
//      logger.info("Sharding:  [{}]", nShard)
//      logger.info("Vertex DB: [{}]", verticesDB)
//      iterations
//      if (data.isEmpty())
//        None
//      else {
//        logger.info("Generating results ...")
//        val result = data.toIterator
//        Some(result)
//      }
//    } else {
//      logger.info("edge list(s) incomplete")
//      None
//    }
//}

//abstract class SimpleAlgorithm[T](context: Context)
//    extends Algorithm[T](context: Context) {
//  val shards = new SimpleShards(context.prefix, context.nShard)
//  val vdb = new VerticesTempDB[T]
//}
//
//abstract class DirectionalAlgorithm[T](context: Context)
//    extends Algorithm[T](context: Context) {
//  val shards = new DirectionalShards(context.prefix, context.nShard)
//  val vdb = new VerticesTempDB[T]
//}
//
//abstract class BidirectionalAlgorithm[T](context: Context)
//    extends Algorithm[T](context: Context) {
//  val shards = new BidirectionalShards(context.prefix, context.nShard)
//  val vdb = new VerticesTempDB[T]
//}