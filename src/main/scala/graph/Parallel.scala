package graph

object Parallel {
  import java.nio.{ ByteBuffer, ByteOrder }
  import java.nio.channels.FileChannel
  import java.nio.file.Paths
  import java.nio.file.StandardOpenOption._
  import scala.collection.mutable.Set
  import akka.actor.{ ActorSystem, Actor, ActorRef, Props }
  import helper.{ GrowingArray, GrowingBitSet }
  import helper.Lines.LinesWrapper
  import helper.Logging

  val as = ActorSystem("SAGE")

  sealed abstract class Message
  case class R2F(i: Int) extends Message
  case class R2P(i: Int) extends Message
  case class START() extends Message
  case class EMPTY(i: Int) extends Message
  case class RESET() extends Message
  case class COMPLETE() extends Message

  class Scanner(
    edgeFileName: String,
    buffers: Array[ByteBuffer])
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

  abstract class Algorithm[E <: Edge, V: Manifest](default: V) extends Logging {
    val vertices = GrowingArray[V](default)
    var stepCounter = 0
    val flags = Array.fill(2)(new GrowingBitSet)
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
    def complete(): Iterator[(Long, V)] = vertices.updated
  }

  class Processor[T](
    buffers: Array[ByteBuffer],
    scanners: Array[ActorRef],
    algorithm: Algorithm[SimpleEdge, T],
    outputFileName: String)
      extends Actor with Logging {
    import Edges.buffer2edges

    val nBuffers = buffers.length
    val nScanners = scanners.length
    require(nBuffers % nScanners == 0 && nBuffers > nScanners)
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
        val edges = buffers(i)
        algorithm.compute(edges)
        sender ! R2F(i)
      case EMPTY(i) =>
        emptyBuf += i
        if (emptyBuf.size == nBuffers) {
          algorithm.update()
          algorithm.forward()
          if (algorithm.hasNext()) {
            emptyBuf.clear()
            logger.debug("next super step")
            scanners.foreach(_ ! RESET)
            self ! START
          } else {
            logger.debug("complete")
            algorithm.complete().map {
              case (k, v: Double) => "%d %.9f".format(k, v)
              case (k, v) => s"$k $v"
            }.toFile(outputFileName)
            scanners.foreach(_ ! COMPLETE)
            sys.exit
          }
        }
      case _ => logger.error("unidentified message")
    }
  }

  class Processor_W[T](
    buffers: Array[ByteBuffer],
    scanners: Array[ActorRef],
    algorithm: Algorithm[WeightedEdge, T],
    outputFileName: String)
      extends Actor with Logging {
    import WEdges.buffer2edges

    val nBuffers = buffers.length
    val nScanners = scanners.length
    require(nBuffers % nScanners == 0 && nBuffers > nScanners)
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
        val edges = buffers(i)
        algorithm.compute(edges)
        sender ! R2F(i)
      case EMPTY(i) =>
        emptyBuf += i
        if (emptyBuf.size == nBuffers) {
          algorithm.update()
          algorithm.forward()
          if (algorithm.hasNext()) {
            emptyBuf.clear()
            logger.debug("next super step")
            scanners.foreach(_ ! RESET)
            self ! START
          } else {
            logger.debug("complete")
            algorithm.complete().map {
              case (k, v: Double) => "%d %.9f".format(k, v)
              case (k, v) => s"$k $v"
            }.toFile(outputFileName)
            scanners.foreach(_ ! COMPLETE)
            sys.exit
          }
        }
      case _ => logger.error("unidentified message")
    }
  }

  class Engine(edgeFileNames: Array[String], outputFileName: String) {
    import settings.Config.{ nBuffersPerScanner, nEdgesPerBuffer }
    import graph.Edges.edgeSize

    val nBytesPerBuffer = edgeSize * nEdgesPerBuffer
    val nScanners = edgeFileNames.length
    val nBuffers = nScanners * nBuffersPerScanner
    val buffers = Array.fill(nBuffers)(ByteBuffer.allocate(nBytesPerBuffer).order(ByteOrder.LITTLE_ENDIAN))

    def run[T](algorithm: Algorithm[SimpleEdge, T]) = {
      val scanners = edgeFileNames.zipWithIndex.map {
        case (edgeFileName, i) =>
          as.actorOf(Props(new Scanner(edgeFileName, buffers)),
            name = s"scanner-$i")
      }
      val processor =
        as.actorOf(Props(new Processor(buffers, scanners, algorithm, outputFileName)),
          name = "processor")

      processor ! START
    }
  }

  class Engine_W(edgeFileNames: Array[String], outputFileName: String) {
    import settings.Config.{ nBuffersPerScanner, nEdgesPerBuffer }
    import graph.WEdges.edgeSize

    val nBytesPerBuffer = edgeSize * nEdgesPerBuffer
    val nScanners = edgeFileNames.length
    val nBuffers = nScanners * nBuffersPerScanner
    val buffers = Array.fill(nBuffers)(ByteBuffer.allocate(nBytesPerBuffer).order(ByteOrder.LITTLE_ENDIAN))

    def run[T](algorithm: Algorithm[WeightedEdge, T]) = {
      val scanners = edgeFileNames.zipWithIndex.map {
        case (edgeFileName, i) =>
          as.actorOf(Props(new Scanner(edgeFileName, buffers)),
            name = s"scanner-$i")
      }
      val processor =
        as.actorOf(Props(new Processor_W(buffers, scanners, algorithm, outputFileName)),
          name = "processor")

      processor ! START
    }
  }
}
