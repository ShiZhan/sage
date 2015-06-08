package sage.test.Edge

object EdgeScanningTest {
  import java.util.Scanner
  import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
  import graph.{ Edge, Edges, EdgeFile }
  import Edges._
  import configuration.Parallel.sageActors
  import helper.HugeContainers.GrowingArray
  import helper.Logging

  sealed abstract class Messages
  case class SCAN(start: Long, count: Long) extends Messages
  case class HALT() extends Messages
  case class DONE(n: Long) extends Messages

  class EdgeScanner(id: Int, edgeFile: EdgeFile, collector: ActorRef) extends Actor with Logging {
    def receive = {
      case SCAN(start, count) =>
        logger.info("SCAN {} {}", (edgeFile.name, (start, count)))
        val edges = edgeFile.getRange(start, count)
        val sum = (0L /: edges) { (r, e) => r + 1 }
        collector ! DONE(sum)
      case HALT =>
        edgeFile.close
        sys.exit
      case _ => logger.error("unidentified message")
    }
  }

  class Collector(total: Int) extends Actor with Logging {
    var counter = total
    var scannedEdges = 0L
    val scanners = context.actorSelection(s"../$total-*")
    val t0 = compat.Platform.currentTime
    def receive = {
      case DONE(n) =>
        logger.info("{} DONE {} edges", sender.path, n)
        scannedEdges += n
        counter -= 1
        if (counter == 0) {
          val t1 = compat.Platform.currentTime
          val t = t1 - t0
          val speed = ((scannedEdges >> 16) * 1000) / t
          logger.info("scanned {} edges in {} ms", scannedEdges, t)
          logger.info("I/O speed: {} MB/s", speed)
          scanners ! HALT
          sys.exit
        }
    }
  }

  def main(args: Array[String]) = args.toList match {
    case edgeFileName :: sScaleString :: Nil =>
      val sScale = sScaleString.toInt
      val nSlice = 1 << sScale
      val sID = 0 to (nSlice - 1)
      val edgeFile = EdgeFile(edgeFileName)
      val eTotal = edgeFile.total
      edgeFile.close
      val sliceSize = eTotal >> sScale
      val collector = sageActors.actorOf(Props(new Collector(nSlice)), name = s"collector-$nSlice")
      val eScanners = sID.map { id =>
        val edgeFile = EdgeFile(edgeFileName)
        sageActors.actorOf(Props(new EdgeScanner(id, edgeFile, collector)), name = s"$nSlice-$id")
      }
      println(s"launching $nSlice scanners")
      val slices = sID.map { id =>
        (id * sliceSize, if (id == (nSlice - 1)) (eTotal & (nSlice - 1)) + sliceSize else sliceSize)
      }.toIterator
      eScanners.foreach { val (s, c) = slices.next; _ ! SCAN(s, c) }
    case _ => println("run with <edge file: String> <slice: Int>")
  }
}
