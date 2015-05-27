package graph

case class Vertex[T](id: Long, value: T)

object Vertices {
  import java.io.File
  import java.util.concurrent.ConcurrentNavigableMap
  import org.mapdb.DBMaker

  type VertexTable[T] = ConcurrentNavigableMap[Long, T]

  abstract class VertexDB[T] {
    def getVertices(mapName: String): VertexTable[T]
  }

  class VertexMemDB[T] extends VertexDB[T] {
    private val db = DBMaker.newMemoryDB().closeOnJvmShutdown().make()
    def commit() = db.commit()
    def close() = db.close()
    def getVertices(mapName: String): VertexTable[T] = db.getTreeMap(mapName)
  }

  class VertexTempDB[T] extends VertexDB[T] {
    private val db = DBMaker.newTempFileDB().closeOnJvmShutdown().make()
    def commit() = db.commit()
    def close() = db.close()
    def getVertices(mapName: String): VertexTable[T] = db.getTreeMap(mapName)
  }

  class VertexFileDB[T](vdbFile: String) extends VertexDB[T] {
    private val file = new File(vdbFile)
    private val db = DBMaker.newFileDB(file).closeOnJvmShutdown().make()
    def commit() = db.commit()
    def close() = db.close()
    def getVertices(mapName: String): VertexTable[T] = db.getTreeMap(mapName)
  }

  def apply[T] = new VertexMemDB[T]
  def apply[T](vdbFile: String) = if (vdbFile.isEmpty) new VertexTempDB[T] else new VertexFileDB[T](vdbFile)
}