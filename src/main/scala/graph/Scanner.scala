package graph

class Scanner(inputFileName: String) extends helper.Logging {
  import java.io.{ BufferedInputStream, File, FileInputStream }
  import java.nio.{ ByteBuffer, ByteOrder }
  import EdgeConverters.Bytes2Edge
  import helper.Gauge.IteratorOperations

  private val inputFile = new File(inputFileName)
  //  private val inputChunks = io.Source.fromFile(inputFile, "ISO-8859-1").map(_.toByte).grouped(0x100000 * 8 * 2) // 16 MB
  private val inputEdges =
    io.Source.fromFile(inputFile, "ISO-8859-1").map(_.toByte).grouped(16).map(_.toArray.toEdge)

  def run(options: List[String]) = {
    logger.info("options [{}]", options)
    inputEdges.foreach(println)
  }
}