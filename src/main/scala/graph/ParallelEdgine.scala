package graph

object ParallelEngine {
  import java.nio.{ ByteBuffer, ByteOrder }
  import java.nio.channels.FileChannel
  import java.nio.file.Paths
  import java.nio.file.StandardOpenOption._
  import scala.collection.mutable.{ Set, BitSet }
  import akka.actor.{ ActorSystem, Actor, ActorRef, Props }
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
        logger.debug("ready to fill {}", i)
        val buf = buffers(i)
        buf.clear()
        while (fc.read(buf) != -1 && buf.hasRemaining) {}
        if (buf.position == 0) sender ! EMPTY(i) else sender ! R2P(i)
      case RESET =>
        logger.debug("rewind to beginning")
        fc.position(0)
      case COMPLETE =>
        logger.debug("close file")
        fc.close()
        sys.exit
      case _ => logger.error("unidentified message")
    }
  }

  abstract class Algorithm[E <: Edge] extends Logging {
    var stepCounter = 0
    val flags = Array.fill(2)(new BitSet)
    def gather = flags(stepCounter & 1)
    def scatter = flags((stepCounter + 1) & 1)
    def forward() = {
      val stat = "[ % 10d -> % 10d ]".format(gather.size, scatter.size)
      gather.clear()
      stepCounter += 1
      logger.info("Step {}: {}", stepCounter, stat)
    }
    def hasNext() = gather.nonEmpty

    def compute(edges: Iterator[E]): Unit
    def update(): Unit
    def complete(): Unit
  }

  class Processor(
    buffers: Array[ByteBuffer], scanners: Array[ActorRef], alg: Algorithm[SimpleEdge])
      extends Actor with Logging {
    import graph.Edges.edgeSize

    val nBuffers = buffers.length
    val nScanners = scanners.length
    require(nBuffers % nScanners == 0)
    val emptyBuf = Set.empty[Int]

    def receive = {
      case START =>
        logger.debug("start")
        for (
          (sId, rIds) <- buffers.indices.groupBy(_ % nScanners);
          s = scanners(sId);
          r <- rIds.map(R2F)
        ) s ! r
      case R2P(i) =>
        logger.debug("ready to process {}", i)
        val buf = buffers(i)
        buf.flip()
        val nEdges = buf.remaining() / edgeSize
        val edges = Iterator.continually { Edge(buf.getInt, buf.getInt) }.take(nEdges)
        alg.compute(edges)
        sender ! R2F(i)
      case EMPTY(i) =>
        emptyBuf += i
        if (emptyBuf.size == nBuffers) {
          alg.update()
          alg.forward()
          if (alg.hasNext()) {
            emptyBuf.clear()
            logger.debug("next super step")
            scanners.foreach(_ ! RESET)
            self ! START
          } else {
            logger.debug("complete")
            alg.complete()
            scanners.foreach(_ ! COMPLETE)
            sys.exit
          }
        }
      case _ => logger.error("unidentified message")
    }
  }

  class Processor_W(
    buffers: Array[ByteBuffer], scanners: Array[ActorRef], alg: Algorithm[WeightedEdge])
      extends Actor with Logging {
    import graph.WEdges.edgeSize

    val nBuffers = buffers.length
    val nScanners = scanners.length
    require(nBuffers % nScanners == 0)
    val emptyBuf = Set.empty[Int]

    def receive = {
      case START =>
        logger.debug("start")
        for (
          (sId, rIds) <- buffers.indices.groupBy(_ % nScanners);
          s = scanners(sId);
          r <- rIds.map(R2F)
        ) s ! r
      case R2P(i) =>
        logger.debug("ready to process {}", i)
        val buf = buffers(i)
        buf.flip()
        val nEdges = buf.remaining() / edgeSize
        val edges = Iterator.continually { Edge(buf.getInt, buf.getInt, buf.getFloat) }.take(nEdges)
        alg.compute(edges)
        sender ! R2F(i)
      case EMPTY(i) =>
        emptyBuf += i
        if (emptyBuf.size == nBuffers) {
          alg.update()
          alg.forward()
          if (alg.hasNext()) {
            emptyBuf.clear()
            logger.debug("next super step")
            scanners.foreach(_ ! RESET)
            self ! START
          } else {
            logger.debug("complete")
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
    import graph.Edges.edgeSize

    val nBuffersPerScanner = 2
    val nEdgesPerBuffer = 1 << 20
    val nBytesPerBuffer = edgeSize * nEdgesPerBuffer
    val nScanners = edgeFileNames.length
    val nBuffers = nScanners * nBuffersPerScanner
    val buffers = Array.fill(nBuffers)(ByteBuffer.allocate(nBytesPerBuffer).order(ByteOrder.LITTLE_ENDIAN))

    def run(alg: Algorithm[SimpleEdge]) = {
      val scanners = edgeFileNames.map { edgeFileName =>
        as.actorOf(Props(new Scanner(edgeFileName, buffers)),
          name = s"scanner-$edgeFileName")
      }
      val processor =
        as.actorOf(Props(new Processor(buffers, scanners, alg)), name = "processor")

      processor ! START
    }
  }

  class Engine_W(edgeFileNames: Array[String]) {
    import graph.WEdges.edgeSize

    val nBuffersPerScanner = 2
    val nEdgesPerBuffer = 1 << 20
    val nBytesPerBuffer = edgeSize * nEdgesPerBuffer
    val nScanners = edgeFileNames.length
    val nBuffers = nScanners * nBuffersPerScanner
    val buffers = Array.fill(nBuffers)(ByteBuffer.allocate(nBytesPerBuffer).order(ByteOrder.LITTLE_ENDIAN))

    def run(alg: Algorithm[WeightedEdge]) = {
      val scanners = edgeFileNames.map { edgeFileName =>
        as.actorOf(Props(new Scanner(edgeFileName, buffers)),
          name = s"scanner-$edgeFileName")
      }
      val processor =
        as.actorOf(Props(new Processor_W(buffers, scanners, alg)), name = "processor")

      processor ! START
    }
  }
}
