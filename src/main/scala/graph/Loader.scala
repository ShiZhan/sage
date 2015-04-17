package graph

class Loader(fileName: Option[String] = None) extends helper.Logging {
  val edgesRaw =
    if (fileName.isDefined) io.Source.fromFile(fileName.get).getLines()
    else io.Source.fromInputStream(System.in).getLines()
}