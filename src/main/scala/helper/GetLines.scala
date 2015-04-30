/**
 * return line iterator
 */
package helper

/**
 * @author Zhan
 * get lines from text file or console
 */
object GetLines {
  import scala.io.Source
  import java.io.File

  def fromConsole =
    Source.fromInputStream(System.in).getLines()

  def fromFile(fileName: String) =
    Source.fromFile(new File(fileName)).getLines()

  def fromFileOrConsole(fileName: String) =
    if (fileName.isEmpty()) fromConsole else fromFile(fileName)
}
