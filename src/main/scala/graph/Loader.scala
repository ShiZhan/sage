package graph

class Edge(from: Long, to: Long, weight: Long) {
  override def toString = if (weight == 1) s"$from $to" else s"$from $to $weight"
}

class Vertex(id: Long)

class Loader(fileName: Option[String] = None) extends helper.Logging {
  val edgesRaw =
    if (fileName.isDefined) io.Source.fromFile(fileName.get).getLines()
    else io.Source.fromInputStream(System.in).getLines()

  val edges = edgesRaw map {
    line =>
      line.split(" ").toList match {
        case from :: to :: Nil if (Seq(from, to).forall { i => i.forall { _.isDigit } }) =>
          new Edge(from.toLong, to.toLong, 1)
        case from :: to :: weight :: Nil if (Seq(from, to, weight).forall { _.forall { d => d.isDigit } }) =>
          new Edge(from.toLong, to.toLong, weight.toLong)
      }
  }
}