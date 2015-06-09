package sage.test.Edge

object EdgeScanningTest {
  import java.util.Scanner
  import scala.collection.mutable.Map
  import akka.actor.{ Actor, ActorRef, Props }
  import graph.{ Edge, Edges, EdgeFile }
  import Edges._
  import configuration.Parallel.sageActors
  import helper.HugeContainers._
  import helper.Lines.LinesWrapper
  import helper.Logging

  sealed abstract class Messages
  case class SCAN(start: Long, count: Long) extends Messages
  case class HALT() extends Messages
  case class DATA(r: List[(Long, Int)]) extends Messages
  case class DONE(n: Long) extends Messages

  class EdgeScanner(id: Int, edgeFile: EdgeFile, collector: ActorRef) extends Actor with Logging {
    val data = Map[Long, Int]()
    def receive = {
      case SCAN(start, count) =>
        val range = "%s: %012d(%012d)".format(edgeFile.name, start, count)
        logger.info("SCAN [{}] {}", id, range)
        val edges = edgeFile.getRange(start, count)
        val sum = (0L /: edges) { (r, e) =>
          val Edge(u, v) = e
          val dU = data.getOrElse(u, 0); data.put(u, dU + 1)
          val dV = data.getOrElse(v, 0); data.put(v, dV + 1)
          if (data.size > (1 << 15)) {
            val buffer = data.toList
            data.clear()
            collector ! DATA(buffer)
          }
          r + 1
        }
        collector ! DATA(data.toList)
        collector ! DONE(sum)
      case HALT =>
        edgeFile.close
        sys.exit
      case _ => logger.error("unidentified message")
    }
  }

  class Collector(total: Int, degree: HugeArray[Int], closing: () => Unit) extends Actor with Logging {
    var counter = total
    var scannedEdges = 0L
    val scanners = context.actorSelection(s"../$total-*")
    val t0 = compat.Platform.currentTime
    def receive = {
      case DATA(data) =>
        for ((k, v) <- data) degree(k) += v
      case DONE(n) =>
        logger.info("{} DONE {} edges", sender.path, n)
        scannedEdges += n
        counter -= 1
        if (counter == 0) {
          scanners ! HALT
          val t1 = compat.Platform.currentTime
          val t = t1 - t0
          val speed = ((scannedEdges >> 16) * 1000) / t
          logger.info("scanned {} edges in {} ms", scannedEdges, t)
          logger.info("I/O speed: {} MB/s", speed)
          closing()
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
      val degree = GrowingArray[Int](0)
      def closing() = { degree.inUse.map { case (k, v) => s"$k $v" }.toFile(edgeFileName + "-degree.out") }
      val collector = sageActors.actorOf(Props(new Collector(nSlice, degree, closing)), name = s"collector-$nSlice")
      val eScanners = sID.map { id =>
        val edgeFile = EdgeFile(edgeFileName)
        sageActors.actorOf(Props(new EdgeScanner(id, edgeFile, collector)), name = s"$nSlice-$id")
      }
      println(s"launching $nSlice scanners")
      sID.foreach { id =>
        val start = id * sliceSize
        val count = if (id == (nSlice - 1)) (eTotal & (nSlice - 1)) + sliceSize else sliceSize
        eScanners(id) ! SCAN(start, count)
      }
    case _ => println("run with <edge file: String> <slice: Int>")
  }
}
