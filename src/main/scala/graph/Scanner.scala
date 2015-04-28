package graph

class Scanner(edgefn: String) extends helper.Logging {
  import java.io.File
  import java.nio.{ ByteBuffer, ByteOrder }
  import EdgeUtils.Bytes2Edge

  private val edgesFile = new File(edgefn)
  private val edges =
    io.Source.fromFile(edgesFile, "ISO-8859-1").map(_.toByte).grouped(16).map(_.toArray.toEdge)

  def run(options: List[String]) = {
    logger.info("options [{}]", options)
    edges.foreach(println) // 0x100000 edges = 1M edges = 16 MB
  }
}

object Scanner {
  def apply(ifn: String) = new Scanner(ifn)
}