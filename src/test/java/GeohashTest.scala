import ch.hsr.geohash.GeoHash

object GeohashTest extends App {


  val hash1  = GeoHash.geoHashStringWithCharacterPrecision(39.87204158807106, 116.40840708459781, 6)
  val hash2  = GeoHash.geoHashStringWithCharacterPrecision(39.556274426445476, 116.91387422753255, 6)

  println(hash1)
  println(hash2)

}
