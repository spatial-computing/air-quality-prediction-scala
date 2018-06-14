package dataDef

import java.sql.Timestamp
import java.text.SimpleDateFormat

class AreaDistance (reportingAreaAStr: String,
                    reportingAreaBStr: String,
                    distanceDouble: Double) extends java.io.Serializable {

    var reportingAreaA = reportingAreaAStr
    var reportingAreaB = reportingAreaBStr
    var distance = distanceDouble
}
