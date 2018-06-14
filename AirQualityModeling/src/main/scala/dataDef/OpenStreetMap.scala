package dataDef;


class OpenStreetMap (_reportingArea: String,
                     _geoFeature: String,
                     _featureType: String,
                     _bufferSize: Int,
                     _value: Double
                    ) extends java.io.Serializable {

  var reportingArea = _reportingArea
  val geoFeature = _geoFeature
  var featureType = _featureType
  var bufferSize = _bufferSize
  var value = _value

}
