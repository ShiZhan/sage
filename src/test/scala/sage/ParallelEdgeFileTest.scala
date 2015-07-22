package sage.test

object ParallelEdgeFileTest {
  import java.nio.{ ByteBuffer, ByteOrder }
  import java.nio.channels.FileChannel
  import java.nio.file.Paths
  import java.nio.file.StandardOpenOption._
  import scala.collection.mutable.{ Map, Set }
  import akka.actor.{ ActorSystem, Actor, ActorRef, Props }
  import graph.{ Edge, SimpleEdge }
  import graph.Edges.edgeSize
  import helper.GrowingArray
  import helper.Logging
  import helper.Lines.LinesWrapper

  val sageActors = ActorSystem("SAGE")

  sealed abstract class Message
  case class R2F(i: Int) extends Message
  case class R2P(i: Int) extends Message
  case class START() extends Message
  case class EMPTY(i: Int) extends Message
  case class RESET() extends Message
  case class COMPLETE() extends Message

  class Scanner(edgeFileName: String, buffers: Array[ByteBuffer])
      extends Actor with Logging {
    val p = Paths.get(edgeFileName)
    val fc = FileChannel.open(p, READ)

    def receive = {
      case R2F(i) =>
        logger.info("ready to fill {}", i)
        val buf = buffers(i)
        buf.clear()
        while (fc.read(buf) != -1 && buf.hasRemaining) {}
        if (buf.position == 0) sender ! EMPTY(i) else sender ! R2P(i)
      case RESET =>
        fc.position(0)
      case COMPLETE =>
        fc.close()
        sys.exit
      case _ => logger.error("unidentified message")
    }
  }

  class Processor(
    buffers: Array[ByteBuffer], scanners: Array[ActorRef],
    loopOp: Iterator[SimpleEdge] => Unit,
    chkRestart: () => Boolean,
    completeOp: () => Unit)
      extends Actor with Logging {
    val nBuffers = buffers.length
    val nScanners = scanners.length
    require(nBuffers % nScanners == 0)
    val emptyBuf = Set[Int]()

    def receive = {
      case START =>
        for (
          (sId, rIds) <- buffers.indices.groupBy(_ % nScanners);
          s = scanners(sId);
          r <- rIds.map(R2F)
        ) s ! r
      case R2P(i) =>
        logger.info("ready to process {}", i)
        val buf = buffers(i)
        buf.flip()
        val nEdges = buf.remaining() / edgeSize
        val edges = Iterator.continually { Edge(buf.getInt, buf.getInt) }.take(nEdges)
        loopOp(edges)
        sender ! R2F(i)
      case EMPTY(i) =>
        emptyBuf += i
        if (emptyBuf.size == nBuffers) {
          if (chkRestart()) {
            logger.info("next loop")
            scanners.foreach(_ ! RESET)
            self ! START
          } else {
            logger.info("complete")
            completeOp()
            scanners.foreach(_ ! COMPLETE)
            sys.exit
          }
        }
      case _ => logger.error("unidentified message")
    }
  }

  def main(args: Array[String]) = if (args.nonEmpty) {
    val nBuffersPerScanner = 2
    val nEdgesPerBuffer = 1 << 20
    val nBytesPerBuffer = edgeSize * nEdgesPerBuffer
    val nScanners = args.length
    val nBuffers = nScanners * nBuffersPerScanner
    val buffers = Array.fill(nBuffers)(ByteBuffer.allocate(nBytesPerBuffer).order(ByteOrder.LITTLE_ENDIAN))
    val degrees = GrowingArray[Int](0)

    def getDegree(edges: Iterator[SimpleEdge]) =
      for (Edge(u, v) <- edges) degrees.synchronized {
        val dU = degrees(u)
        val dV = degrees(v)
        degrees(u) = dU + 1
        degrees(v) = dV + 1
      }

    def moreLoop() = false

    def printDegree() =
      degrees.synchronized { degrees.updated.map { case (k, v) => s"$k $v" }.toFile("test.csv") }

    val scanners = args.map { edgeFileName =>
      sageActors.actorOf(Props(new Scanner(edgeFileName, buffers)),
        name = s"scanner-$edgeFileName")
    }
    val processor =
      sageActors.actorOf(Props(new Processor(buffers, scanners, getDegree, moreLoop, printDegree)),
        name = "processor")

    processor ! START
  } else println("run with <edge file(s)>")
}
