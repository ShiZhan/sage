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
        logger.info("rewind to beginning")
        fc.position(0)
      case COMPLETE =>
        logger.info("close file")
        fc.close()
        sys.exit
      case _ => logger.error("unidentified message")
    }
  }

  abstract class Algorithm extends Logging {
    var stepCounter = 0
    val flags = Array.fill(2)(new BitSet)
    def gather = flags(stepCounter & 1)
    def scatter = flags((stepCounter + 1) & 1)
    def update() = {
      val stat = "[ % 10d -> % 10d ]".format(gather.size, scatter.size)
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
        logger.info("start")
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
          alg.update()
          if (alg.more()) {
            emptyBuf.clear()
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
  import helper.Timing._

  case class DirectedDegree(i: Int, o: Int) {
    def addIDeg = DirectedDegree(i + 1, o)
    def addODeg = DirectedDegree(i, o + 1)
    override def toString = s"$i $o"
  }

  class Degree extends Algorithm {
    val degree = GrowingArray[DirectedDegree](DirectedDegree(0, 0))

    def loop(edges: Iterator[SimpleEdge]) =
      for (Edge(u, v) <- edges) degree.synchronized {
        degree(u) = degree(u).addODeg
        degree(v) = degree(u).addIDeg
      }

    def more() = false

    def complete() =
      degree.synchronized { degree.updated.map { case (k, v) => s"$k $v" }.toFile("degree.csv") }
  }

  class Degree_U extends Algorithm {
    val degree = GrowingArray[Int](0)

    def loop(edges: Iterator[SimpleEdge]) =
      for (Edge(u, v) <- edges) degree.synchronized {
        degree(u) = degree(u) + 1
        degree(v) = degree(v) + 1
      }

    def more() = false

    def complete() =
      degree.synchronized { degree.updated.map { case (k, v) => s"$k $v" }.toFile("degree-u.csv") }
  }

  class BFS(root: Int) extends Algorithm {
    val distance = GrowingArray[Int](0)
    var d = 1
    distance(root) = d
    gather.add(root)

    def loop(edges: Iterator[SimpleEdge]) =
      for (Edge(u, v) <- edges if (gather(u) && distance.unVisited(v))) distance.synchronized {
        distance(v) = d; scatter.add(v)
      }

    def more() = { d += 1; gather.nonEmpty }

    def complete() =
      distance.synchronized { distance.updated.map { case (k, v) => s"$k $v" }.toFile("bfs.csv") }
  }

  class BFS_U(root: Int) extends Algorithm {
    val distance = GrowingArray[Int](0)
    var d = 1
    distance(root) = d
    gather.add(root)

    def loop(edges: Iterator[SimpleEdge]) =
      for (Edge(u, v) <- edges) distance.synchronized {
        if (gather(u) && distance.unVisited(v)) { distance(v) = d; scatter.add(v) }
        if (gather(v) && distance.unVisited(u)) { distance(u) = d; scatter.add(u) }
      }

    def more() = { d += 1; gather.nonEmpty }

    def complete() =
      distance.synchronized { distance.updated.map { case (k, v) => s"$k $v" }.toFile("bfs-u.csv") }
  }

  def main(args: Array[String]) =
    if (args.nonEmpty) {
      val e0 = { () =>
        implicit val eps = args.map(new graph.SimpleEdgeFile(_)).toSeq
        val result = new algorithms.parallel.BFS_U(0).run
        eps.foreach(_.close)
        result.map { case (k: Int, v: Any) => s"$k $v" }.toFile("bfs-u-reference.csv")
      }.elapsed
      println(s"reference run $e0 ms")
      new Engine(args).run(new BFS_U(0))
    } else println("run with <edge file(s)>")
}
