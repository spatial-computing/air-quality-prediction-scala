package util

object Constants {

  // Pre-define k for each monitoring station in each temporal scale
  val kHourlyMap = Map("Central LA CO"-> 6, "W San Gabriel Vly" -> 7, "San Gabriel Mts" -> 7, "SW San Bernardino" -> 7,
    "W San Fernando Vly" -> 6, "E San Fernando Vly" -> 6, "E San Gabriel V-2" -> 5, "NW Coastal LA" -> 7,
    "Santa Clarita Vly" -> 8, "SW Coastal LA" -> 7, "South Coastal LA" -> 8, "Southeast LA CO" -> 7)

  val kDailyMap = Map("Central LA CO"-> 7, "W San Gabriel Vly" -> 4, "San Gabriel Mts" -> 4, "SW San Bernardino" -> 5,
    "W San Fernando Vly" -> 6, "E San Fernando Vly" -> 5, "E San Gabriel V-2" -> 5, "NW Coastal LA" -> 6,
    "Santa Clarita Vly" -> 5, "SW Coastal LA" -> 6, "South Coastal LA" -> 6, "Southeast LA CO" -> 4)

  val kMonthlyMap = Map("Central LA CO"-> 8, "W San Gabriel Vly" -> 5, "San Gabriel Mts" -> 5, "SW San Bernardino" -> 5,
    "W San Fernando Vly" -> 5, "E San Fernando Vly" -> 5, "E San Gabriel V-2" -> 5, "NW Coastal LA" -> 6,
    "Santa Clarita Vly" -> 4, "SW Coastal LA" -> 6, "South Coastal LA" -> 6, "Southeast LA CO" -> 5)

  val bufferSize = Array(100, 200, 300, 400, 500, 600, 700, 800, 900, 1000,
    1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000,
    2100, 2200, 2300, 2400, 2500, 2600, 2700, 2800, 2900, 3000)

}
