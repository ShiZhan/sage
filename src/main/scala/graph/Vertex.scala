package graph

class Vertices[ValueType](verticesFile: String) {
  import java.io.File
  import java.util.concurrent.ConcurrentNavigableMap
  import org.mapdb.DBMaker

  type VertexTable = ConcurrentNavigableMap[Long, ValueType]

  private val db =
    if (verticesFile.isEmpty)
      DBMaker.newTempFileDB().closeOnJvmShutdown().make()
    else
      DBMaker.newFileDB(new File(verticesFile)).closeOnJvmShutdown().make()
  def commit() = db.commit()
  def close() = db.close()

  def getVertexTable(name: String) =
    db.getTreeMap(name).asInstanceOf[VertexTable]
}