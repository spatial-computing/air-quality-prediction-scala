package Utils

object Consts {

  // Pre-define k for each monitoring station in each temporal scale
  val kHourlyMap = Map("Central LA CO"-> 8, "W San Gabriel Vly" -> 8, "San Gabriel Mts" -> 8, "SW San Bernardino" -> 8,
    "W San Fernando Vly" -> 9, "E San Fernando Vly" -> 9, "E San Gabriel V-2" -> 8, "NW Coastal LA" -> 9,
    "Santa Clarita Vly" -> 8, "SW Coastal LA" -> 9, "South Coastal LA" -> 9, "Southeast LA CO" -> 8)

  val kDailyMap = Map("Central LA CO"-> 8, "W San Gabriel Vly" -> 8, "San Gabriel Mts" -> 8, "SW San Bernardino" -> 8,
    "W San Fernando Vly" -> 9, "E San Fernando Vly" -> 9, "E San Gabriel V-2" -> 8, "NW Coastal LA" -> 9,
    "Santa Clarita Vly" -> 8, "SW Coastal LA" -> 9, "South Coastal LA" -> 9, "Southeast LA CO" -> 8)

  val kMonthlyMap = Map("Central LA CO"-> 8, "W San Gabriel Vly" -> 8, "San Gabriel Mts" -> 8, "SW San Bernardino" -> 8,
    "W San Fernando Vly" -> 9, "E San Fernando Vly" -> 9, "E San Gabriel V-2" -> 8, "NW Coastal LA" -> 9,
    "Santa Clarita Vly" -> 8, "SW Coastal LA" -> 9, "South Coastal LA" -> 9, "Southeast LA CO" -> 8)

  val slc_kMonthlyMap = Map("490351001"-> 3, "490353006" -> 3, "490353010" -> 3, "490353013" -> 2, "490450004" -> 2)

  val bufferSize = Array(100, 200, 300, 400, 500, 600, 700, 800, 900, 1000,
    1100, 1200, 1300, 1400, 1500, 1600, 1700, 1800, 1900, 2000,
    2100, 2200, 2300, 2400, 2500, 2600, 2700, 2800, 2900, 3000)

  val distanceFilename = "./data/fishnetDistance.txt"
  val fishnetFilename = "./data/LA_0_5miles.txt"

  val los_angeles = "los_angeles"
  val salt_lake_city = "salt_lake_city"

  val hourly = "hourly"
  val daily = "daily"
  val monthly = "monthly"

  val iter = 200

}
