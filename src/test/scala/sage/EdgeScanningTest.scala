package sage.test.Edge

object EdgeScanningTest {
  import java.util.Scanner
  import akka.actor.{ Actor, ActorRef, ActorSystem, Props }
  import graph.{ Edge, Edges, EdgeFile }
  import Edges._
  import helper.HugeContainers.GrowingArray
  import helper.Logging

  val system = ActorSystem("SAGE")

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
    var t = total
    var scannedEdges = 0
    val scanners = context.actorSelection(s"../$total-*")
    def receive = {
      case DONE(n) =>
        logger.info("[{}] DONE", sender.path)
        scannedEdges += n
        t -= 1
        if (t == 0) {
          logger.info("scanned [{}] edges", scannedEdges)
          scanners ! HALT
          sys.exit
        }
    }
  }

  def main(args: Array[String]) = {
    val sc = new Scanner(System.in)
    println("input edge total as power of 2:")
    val eTotal = 1 << sc.nextInt
    println("input slice of edges/scanners:")
    val n = sc.nextInt
    val edges = { var v = -1; Iterator.continually { v += 1; Edge(v, v + 1) }.take(eTotal) }
    val slices = edges.grouped(eTotal >> n)
    val total = 1 << n
    println(s"preparing $eTotal edges in $total files")
    slices.zipWithIndex.foreach { case (edges, id) => edges.toIterator.toFile(s"$total-$id.bin") }
    val edgeFiles = (0 to (total - 1)).map(id => s"$total-$id.bin").map(EdgeFile)
    val collector = system.actorOf(Props(new Collector(total)), name = s"collector-$total")
    val eScanners = edgeFiles.map { edgeFile => system.actorOf(Props(new EdgeScanner(edgeFile, collector)), name = edgeFile.name) }
    println(s"launching $total scanners")
    eScanners.foreach { _ ! SCAN }
  }
}
