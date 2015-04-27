package graph

class Importer(edgefn: String) extends helper.Logging {
  import java.io.{ BufferedOutputStream, File, FileOutputStream }
  import java.nio.{ ByteBuffer, ByteOrder }
  import helper.Gauge.IteratorOperations

  private def line2edges(line: String) = line.split(" ").toList match {
    case "#" :: tail =>
      logger.debug("comment: [{}]", line)
      Edge()
    case from :: to :: Nil => Edge(from.toLong, to.toLong)
    case _ =>
      logger.error("invalid: [{}]", line)
      Edge()
  }

  def run = if (edgefn.isEmpty()) {
    val ofn = "graph-%s.bin".format(compat.Platform.currentTime)
    val ofs = new BufferedOutputStream(new FileOutputStream(new File(ofn)))
    io.Source.fromInputStream(System.in).getLines().map(line2edges).filter(_.valid).foreachDo { e =>
      ofs.write(e.toBytes)
    }
    ofs.close()
  } else {
    val ofn = edgefn + ".bin"
    val ofs = new BufferedOutputStream(new FileOutputStream(new File(ofn)))
    logger.info("[{}]", edgefn)
    io.Source.fromFile(edgefn).getLines().map(line2edges).filter(_.valid).foreachDo { e =>
      ofs.write(e.toBytes)
    }
    ofs.close()
  }
}

object Importer {
  def apply() = new Importer("")
  def apply(ifn: String) = new Importer(ifn)
}