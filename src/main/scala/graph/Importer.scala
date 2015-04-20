package graph

object Importer extends helper.Logging {
  import java.io.{ BufferedOutputStream, File, FileOutputStream }
  import java.nio.{ ByteBuffer, ByteOrder }
  import helper.Gauge.IteratorOperations

  private def line2bin(line: String) = line.split(" ").toList match {
    case "#" :: tail =>
      logger.debug("comment: [{}]", line)
      Array[Byte]()
    case from :: to :: Nil =>
      ByteBuffer.allocate(8 * 2).order(ByteOrder.LITTLE_ENDIAN).putLong(from.toLong).putLong(to.toLong).array()
    case _ =>
      logger.error("invalid: [{}]", line)
      Array[Byte]()
  }

  def console2bin = {
    val ofn = "graph-%s.bin".format(compat.Platform.currentTime)
    val ofs = new BufferedOutputStream(new FileOutputStream(new File(ofn)))
    io.Source.fromInputStream(System.in).getLines().map(line2bin).filterNot(_.isEmpty).foreachDo(ofs.write)
    ofs.close()
  }

  def file2bin(inputFileName: String) = {
    val ofn = inputFileName + ".bin"
    val ofs = new BufferedOutputStream(new FileOutputStream(new File(ofn)))
    io.Source.fromFile(inputFileName).getLines().map(line2bin).filterNot(_.isEmpty).foreachDo(ofs.write)
    ofs.close()
  }
}