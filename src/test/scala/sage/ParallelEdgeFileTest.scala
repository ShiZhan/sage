package sage.test

object ParallelEngine {
  import java.nio.{ ByteBuffer, ByteOrder }
  import java.nio.channels.FileChannel
  import java.nio.file.Paths
  import java.nio.file.StandardOpenOption._
  import scala.collection.mutable.{ Set, BitSet }
  import akka.actor.{ ActorSystem, Actor, ActorRef, Props }
  import graph.{ Edge, SimpleEdge }
  import graph.Edges.edgeSize
  import helper.Logging

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

  abstract class Algorithm extends Logging {
    var stepCounter = 0
    val flags = Array.fill(2)(new BitSet)
    def scatter = flags(stepCounter & 1)
    def gather = flags((stepCounter + 1) & 1)
    def update() = {
      val stat = "[ % 10d -> % 10d ]".format(scatter.size, gather.size)
      gather.clear()
      stepCounter += 1
      logger.info("Step {}: {}", stepCounter, stat)
    }

    def loop(edges: Iterator[SimpleEdge]): Unit
    def more(): Boolean
    def complete(): Unit
  }

  class Processor(
    buffers: Array[ByteBuffer], scanners: Array[ActorRef], alg: Algorithm)
      extends Actor with Logging {
    val nBuffers = buffers.length
    val nScanners = scanners.length
    require(nBuffers % nScanners == 0)
    val emptyBuf = Set.empty[Int]

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
        alg.loop(edges)
        sender ! R2F(i)
      case EMPTY(i) =>
        emptyBuf += i
        if (emptyBuf.size == nBuffers) {
          if (alg.more()) {
            alg.update()
            logger.info("next loop")
            scanners.foreach(_ ! RESET)
            self ! START
          } else {
            logger.info("complete")
            alg.complete()
            scanners.foreach(_ ! COMPLETE)
            sys.exit
          }
        }
      case _ => logger.error("unidentified message")
    }
  }

  val as = ActorSystem("SAGE")

  class Engine(edgeFileNames: Array[String]) {
    val nBuffersPerScanner = 2
    val nEdgesPerBuffer = 1 << 20
    val nBytesPerBuffer = edgeSize * nEdgesPerBuffer
    val nScanners = edgeFileNames.length
    val nBuffers = nScanners * nBuffersPerScanner
    val buffers = Array.fill(nBuffers)(ByteBuffer.allocate(nBytesPerBuffer).order(ByteOrder.LITTLE_ENDIAN))

    def run(alg: Algorithm) = {
      val scanners = edgeFileNames.map { edgeFileName =>
        as.actorOf(Props(new Scanner(edgeFileName, buffers)),
          name = s"scanner-$edgeFileName")
      }
      val processor =
        as.actorOf(Props(new Processor(buffers, scanners, alg)), name = "processor")

      processor ! START
    }
  }
}

object ParallelEdgeFileTest {
  import graph.{ Edge, SimpleEdge }
  import ParallelEngine.{ Algorithm, Engine }
  import helper.GrowingArray
  import helper.Lines.LinesWrapper

  case class DirectedDegree(i: Int, o: Int) {
    def addIDeg = DirectedDegree(i + 1, o)
    def addODeg = DirectedDegree(i, o + 1)
    override def toString = s"$i $o"
  }

  class Degree extends Algorithm {
    val degrees = GrowingArray[DirectedDegree](DirectedDegree(0, 0))

    def loop(edges: Iterator[SimpleEdge]) =
      for (Edge(u, v) <- edges) degrees.synchronized {
        degrees(u) = degrees(u).addODeg
        degrees(v) = degrees(u).addIDeg
      }

    def more() = false

    def complete() =
      degrees.synchronized { degrees.updated.map { case (k, v) => s"$k $v" }.toFile("test.csv") }
  }

  class Degree_U extends Algorithm {
    val degrees = GrowingArray[Int](0)

    def loop(edges: Iterator[SimpleEdge]) =
      for (Edge(u, v) <- edges) degrees.synchronized {
        degrees(u) = degrees(u) + 1
        degrees(v) = degrees(v) + 1
      }

    def more() = false

    def complete() =
      degrees.synchronized { degrees.updated.map { case (k, v) => s"$k $v" }.toFile("test.csv") }
  }

  def main(args: Array[String]) =
    if (args.nonEmpty) new Engine(args).run(new Degree)
    else println("run with <edge file(s)>")
}
