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
  case class SCAN() extends Messages
  case class HALT() extends Messages
  case class DONE(n: Int) extends Messages

  class EdgeScanner(edgeFile: EdgeFile, collector: ActorRef) extends Actor with Logging {
    def receive = {
      case SCAN =>
        logger.info("scanning [{}]", edgeFile.name)
        val edges = edgeFile.get
        val sum = (0 /: edges) { (r, e) => r + 1 }
        collector ! DONE(sum)
      case HALT =>
        edgeFile.close
        sys.exit
      case _ => logger.error("unidentified message")
    }
  }

  class Collector(total: Int) extends Actor with Logging {
    var counter = total
    var scannedEdges = 0
    val scanners = context.actorSelection(s"../$total-*")
    val t0 = compat.Platform.currentTime
    def receive = {
      case DONE(n) =>
        logger.info("[{}] DONE", sender.path)
        scannedEdges += n
        counter -= 1
        if (counter == 0) {
          val t1 = compat.Platform.currentTime
          val t = t1 - t0
          val speed = ((scannedEdges >> 16) * 1000) / t
          logger.info("scanned [{}] edges in [{}] ms", scannedEdges, t)
          logger.info("I/O speed: [{}] MB/s", speed)
          scanners ! HALT
          sys.exit
        }
    }
  }

  def main(args: Array[String]) = {
    if (args.isEmpty) {
      val sc = new Scanner(System.in)
      println("input edge total as power of 2:")
      val eScale = sc.nextInt
      val eTotal = 1 << eScale
      println("input slice of edges/scanners:")
      val sScale = sc.nextInt
      val edges = { var v = -1; Iterator.continually { v += 1; Edge(v, v + 1) }.take(eTotal) }
      val sliceSize = eTotal >> sScale
      val nSlice = 1 << sScale
      val sID = 0 to (nSlice - 1)
      println(s"preparing $eTotal edges in $nSlice files")
      (edges /: sID) { (slice, id) => slice.take(sliceSize).toFile(s"$nSlice-$id.bin"); slice }
      val edgeFiles = sID.map(id => s"$nSlice-$id.bin").map(EdgeFile)
      val collector = sageActors.actorOf(Props(new Collector(nSlice)), name = s"collector-$nSlice")
      val eScanners = edgeFiles.map { edgeFile =>
        sageActors.actorOf(Props(new EdgeScanner(edgeFile, collector)), name = edgeFile.name)
      }
      println(s"launching $nSlice scanners")
      eScanners.foreach { _ ! SCAN }
    } else {
      val edgeFileName = args(0)
      val sScale = args(1).toInt
      val nSlice = 1 << sScale
      val sID = 0 to (nSlice - 1)
      val edgeFile = EdgeFile(edgeFileName)
      val eTotal = edgeFile.total
      edgeFile.close
      val sliceSize = eTotal >> sScale
      val collector = sageActors.actorOf(Props(new Collector(nSlice)), name = s"collector-$nSlice")
      val eScanners = sID.map { id =>
        sageActors.actorOf(Props(new EdgeScanner(EdgeFile(edgeFileName), collector)), name = s"$nSlice-$id")
      }
      println(s"launching $nSlice scanners")
      eScanners.foreach { _ ! SCAN }
    }
  }
}
