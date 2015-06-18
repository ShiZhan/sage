package configuration

/**
 * @author ShiZhan
 * parsing program options and prepare default configuration
 */
object Options {
  type OptionMap = Map[Symbol, Any]
  class OptionMapWrapper(om: OptionMap) {
    def isEmpty = om.isEmpty
    def getBool(s: Symbol) = om.contains(s)
    def getString(s: Symbol, d: String) = om.getOrElse(s, d).asInstanceOf[String]
    def getInt(s: Symbol, d: Int) = om.getOrElse(s, d).asInstanceOf[Int]
    def getSpecifiedInt(s: Symbol, d: Int)(checker: Int => Boolean) = om.get(s) match {
      case Some(value: Int) if checker(value) => value
      case _ => d
    }
  }

  private def isSwitch(s: String) = s.startsWith("-")
  private def nextOption(map: OptionMap, optList: List[String]): OptionMap =
    optList match {
      case "-h" :: more =>
        nextOption(map ++ Map('help -> true), more)
      case "-i" :: more =>
        nextOption(map ++ Map('import -> true), more)
      case "-p" :: algorithm :: more =>
        nextOption(map ++ Map('process -> algorithm), more)
      case "-m" :: mapFile :: more =>
        nextOption(map ++ Map('mfile -> mapFile), more)
      case "-g" :: generator :: more =>
        nextOption(map ++ Map('generate -> generator), more)
      case "--binary" :: more =>
        nextOption(map ++ Map('binary -> true), more)
      case "--self-loop" :: more =>
        nextOption(map ++ Map('selfloop -> true), more)
      case "--bidirectional" :: more =>
        nextOption(map ++ Map('bidirectional -> true), more)
      case "--weight" :: more =>
        nextOption(map ++ Map('weight -> true), more)
      case edgeFile :: opt :: more if isSwitch(opt) =>
        nextOption(map ++ Map('efile -> edgeFile), optList.tail)
      case edgeFile :: Nil => map ++ Map('efile -> edgeFile)
      case _ => map
    }

  def getOptions(optList: List[String]) = {
    val options = nextOption(Map(), optList)
    new OptionMapWrapper(options)
  }
}