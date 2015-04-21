package graph

case class Edge(u: Long, v: Long) {
  override def toString = s"$u $v"
}

class Processer(inputFileName: String) extends helper.Logging {
  import java.io.{ BufferedInputStream, File, FileInputStream }
  import java.nio.{ ByteBuffer, ByteOrder }
  import java.util.concurrent.ConcurrentNavigableMap
  import org.mapdb.DBMaker
  import helper.Gauge.IteratorOperations

  private val db = DBMaker.newFileDB(new File(".sage-data")).closeOnJvmShutdown().make()
  private val vertices = db.getTreeMap("vertices").asInstanceOf[ConcurrentNavigableMap[Long, Long]]
  private val inputFile = new File(inputFileName)
  //  private val inputChunks = io.Source.fromFile(inputFile, "ISO-8859-1").map(_.toByte).grouped(0x100000 * 8 * 2) // 16 MB
  private val inputEdges =
    io.Source.fromFile(inputFile, "ISO-8859-1").map(_.toByte).grouped(8*2).map { edgeRaw =>
      val buf = ByteBuffer.wrap(edgeRaw.toArray).order(ByteOrder.LITTLE_ENDIAN)
      Edge(buf.getLong, buf.getLong)
    }

  def run(options: List[String]) = {
    logger.info("operation [{}]", options.head)
    inputEdges.foreach(println)
  }

  def shutdown() = db.close()
}