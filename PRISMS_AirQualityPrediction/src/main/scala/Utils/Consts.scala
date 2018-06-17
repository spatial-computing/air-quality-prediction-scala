package Utils

object Consts {

  val airnow_reporting_area_geofeature_tablename = Map(
    "landuse_a" -> "geo_features.airnow_reporting_areas_geofeature_landuse_a",
    "natural" -> "geo_features.airnow_reporting_areas_geofeature_natural",
    "natural_a" -> "geo_features.airnow_reporting_areas_geofeature_natural_a",
    "places" -> "geo_features.airnow_reporting_areas_geofeature_places",
    "places_a" -> "geo_features.airnow_reporting_areas_geofeature_places_a",
    "pofw" -> "geo_features.airnow_reporting_areas_geofeature_pofw_a",
    "pofw_a" ->"geo_features.airnow_reporting_areas_geofeature_pois",
    "pois" -> "geo_features.airnow_reporting_areas_geofeature_pofw",
    "pois_a" -> "geo_features.airnow_reporting_areas_geofeature_pois_a",
    "railways" -> "geo_features.airnow_reporting_areas_geofeature_railways",
    "roads" -> "geo_features.airnow_reporting_areas_geofeature_roads",
    "traffic" -> "geo_features.airnow_reporting_areas_geofeature_traffic_a",
    "traffic_a" -> "geo_features.airnow_reporting_areas_geofeature_transport",
    "transport" -> "geo_features.airnow_reporting_areas_geofeature_traffic",
    "transport_a" -> "geo_features.airnow_reporting_areas_geofeature_transport_a",
    "waterways" -> "geo_features.airnow_reporting_areas_geofeature_waterways",
    "water_a" -> "geo_features.airnow_reporting_areas_geofeature_water_a",
    "elevation" -> "geo_features.airnow_reporting_areas_elevation",
    "longitude" -> "geo_features.airnow_reporting_areas_elevation",
    "latitude" -> "geo_features.airnow_reporting_areas_elevation"
  )

  val la_fishnet_geofeature_tablename = Map(
    "landuse_a" -> "geo_features.la_fishnet_geofeature_landuse_a",
    "natural" -> "geo_features.la_fishnet_geofeature_natural",
    "natural_a" -> "geo_features.la_fishnet_geofeature_natural_a",
    "places" -> "geo_features.la_fishnet_geofeature_places",
    "places_a" -> "geo_features.la_fishnet_geofeature_places_a",
    "pofw" -> "geo_features.la_fishnet_geofeature_pofw",
    "pofw_a" -> "geo_features.la_fishnet_geofeature_pofw_a",
    "pois" -> "geo_features.la_fishnet_geofeature_pois",
    "pois_a" -> "geo_features.la_fishnet_geofeature_pois_a",
    "railways" -> "geo_features.la_fishnet_geofeature_railways",
    "roads" -> "geo_features.la_fishnet_geofeature_roads",
    "traffic" -> "geo_features.la_fishnet_geofeature_traffic",
    "traffic_a" -> "geo_features.la_fishnet_geofeature_traffic_a",
    "transport" -> "geo_features.la_fishnet_geofeature_transport",
    "transport_a" -> "geo_features.la_fishnet_geofeature_transport_a",
    "waterways" -> "geo_features.la_fishnet_geofeature_waterways",
    "water_a" -> "geo_features.la_fishnet_geofeature_water_a",
    "elevation" -> "geo_features.la_fishnet_elevation",
    "longitude" -> "geo_features.la_fishnet_elevation",
    "latitude" -> "geo_features.la_fishnet_elevation"
  )

  val los_angeles_geofeature_tablename = Map(
    "aeroways" -> "los_angeles.los_angeles_sensor_geofeature_aeroways",
    "buildings" -> "los_angeles.los_angeles_sensor_geofeature_buildings",
    "landusages" -> "los_angeles.los_angeles_sensor_geofeature_landusages",
    "ocean" -> "los_angeles.los_angeles_sensor_geofeature_ocean",
    "roads" -> "los_angeles.los_angeles_sensor_geofeature_roads",
    "waterareas" -> "los_angeles.los_angeles_sensor_geofeature_waterareas",
    "longitude" -> "airnow_reporting_area_location",
    "latitude" -> "airnow_reporting_area_location"
  )

  val los_angeles_fishnet_geofeature_tablename = Map(
    "aeroways" -> "los_angeles.los_angeles_fishnet_geofeature_aeroways",
    "buildings" -> "los_angeles.los_angeles_fishnet_geofeature_buildings",
    "landusages" -> "los_angeles.los_angeles_fishnet_geofeature_landusages",
    "ocean" -> "los_angeles.los_angeles_fishnet_geofeature_ocean",
    "roads" -> "los_angeles.los_angeles_fishnet_geofeature_roads",
    "waterareas" -> "los_angeles.los_angeles_fishnet_geofeature_waterareas",
    "longitude" -> "airnow_reporting_area_location",
    "latitude" -> "airnow_reporting_area_location"
  )
}
