package dataDef

class Feature (_geoFeature: GeoFeature,
               _climateFeature: ClimateFeature) extends java.io.Serializable {

  var geoFeatures = _geoFeature
  var climateFeature = _climateFeature

}
