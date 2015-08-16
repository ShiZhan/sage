package settings

/**
 * @author ShiZhan
 * parsing config file
 */
object Config {
  import java.io.File
  import com.typesafe.config.ConfigFactory

  val config =
    ConfigFactory.parseFile(new File("sage.conf")).withFallback(ConfigFactory.load())
  val nBuffersPerScanner = config.getInt("buffer.nBuffersPerScanner")
  val nEdgesPerBuffer = config.getInt("buffer.nEdgesPerBuffer")
}
