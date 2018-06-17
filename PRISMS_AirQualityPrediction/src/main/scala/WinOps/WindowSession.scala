package WinOps

import org.apache.spark.sql.expressions.{Window, WindowSpec}

class WindowSession (partitionByKey: String, orderbyKey: String,
                     extraPartitionByKeys: List[String] = List[String](),
                     extraOrderByKeys: List[String] = List[String]()) {

  private var rowStart : Long = 0L
  private var rowEnd : Long = 0L
  private var rangeStart : Long = 0L
  private var rangeEnd : Long = 0L
  private var windowSize : Long = 0L
  private var windowSpec : Option[WindowSpec] = None

  def createWindowSpecByRows(K: Int) : WindowSpec =  {

    windowSize = K
    val rowNum = (K - 1) / 2
    rowStart = -rowNum
    rowEnd = rowNum

    val win = Window.partitionBy(partitionByKey, extraPartitionByKeys : _*)
              .orderBy(orderbyKey, extraOrderByKeys : _*)
              .rowsBetween(rowStart, rowEnd)

    windowSpec = Some(win)
    windowSpec.get
  }

  def createWindowSpecByRange(size: Int) : WindowSpec =  {

    windowSize = size
    rangeStart = -windowSize
    rangeEnd = windowSize

    val win = Window.partitionBy(partitionByKey, extraPartitionByKeys : _*)
              .orderBy(orderbyKey, extraOrderByKeys : _*)
              .rangeBetween(rangeStart, rowEnd)

    windowSpec = Some(win)
    windowSpec.get

  }

  def getRowStart : Long = rowStart
  def getRowEnd : Long = rowEnd
  def getRangeStart : Long = rangeStart
  def getRangeEnd : Long = rangeEnd
  def getWindowSize : Long = windowSize
  def getWindowSpec : WindowSpec = windowSpec.get
}