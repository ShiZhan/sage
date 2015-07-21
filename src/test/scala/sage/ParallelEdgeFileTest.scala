package sage.test

object ParallelEdgeFileTest {
  import java.nio.{ ByteBuffer, ByteOrder }
  import java.nio.channels.FileChannel
  import java.nio.file.Paths
  import java.nio.file.StandardOpenOption._
  import scala.collection.mutable.Map
  import akka.actor.{ ActorSystem, Actor, ActorRef, Props }
  import graph.{ Edge, SimpleEdge }
  import graph.Edges.edgeSize
  import helper.Logging

  val sageActors = ActorSystem("SAGE")
  val managerName = "sage-manager"
  val managerNameRelative = s"../$managerName"

  sealed abstract class Message
  case class R2F(i: Int) extends Message
  case class R2P(i: Int) extends Message
  case class START() extends Message
  case class COMPLETE() extends Message

  class Scanner(edgeFileName: String, buffers: Seq[ByteBuffer])
      extends Actor with Logging {
    val p = Paths.get(edgeFileName)
    val fc = FileChannel.open(p, READ)

    def receive = {
      case R2F(i) =>
        logger.info("ready to fill {}", i)
        val buf = buffers(i)
        buf.clear()
        while (fc.read(buf) != -1 && buf.hasRemaining) {}
        if (buf.position == 0) sender ! COMPLETE else sender ! R2P(i)
      case COMPLETE =>
        fc.close()
        sys.exit
      case _ => logger.error("unidentified message")
    }
  }

  class Processor(buffers: Seq[ByteBuffer], scanner: ActorRef, shared: Map[Int, Int])
      extends Actor with Logging {
    def receive = {
      case START => buffers.indices.map(R2F).foreach(scanner ! _)
      case R2P(i) =>
        logger.info("ready to process {}", i)
        val buf = buffers(i)
        buf.flip()
        val nEdges = buf.remaining() / edgeSize
        val edges = Iterator.continually { Edge(buf.getInt, buf.getInt) }.take(nEdges)
        for (Edge(u, v) <- edges) shared.synchronized {
          val dU = shared.getOrElse(u, 0)
          val dV = shared.getOrElse(v, 0)
          shared.put(u, dU + 1)
          shared.put(v, dV + 1)
        }
        scanner ! R2F(i)
      case COMPLETE =>
        shared.synchronized { shared.map { case (k, v) => s"$k $v" }.foreach(println) }
        scanner ! COMPLETE
        sys.exit
      case _ => logger.error("unidentified message")
    }
  }

  def main(args: Array[String]) = args.toList match {
    case edgeFileName :: Nil =>
      val buffers = Seq.fill(8)(ByteBuffer.allocate(edgeSize * 32768).order(ByteOrder.LITTLE_ENDIAN))
      val scanner = sageActors.actorOf(Props(new Scanner(edgeFileName, buffers)),
        name = "scanner")
      val processor = sageActors.actorOf(Props(new Processor(buffers, scanner, Map[Int, Int]())),
        name = "processor")
      processor ! START
    case _ => println("run with <edge file>")
  }
}
