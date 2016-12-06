package geocoding

/**
 * Created by batman on 11/11/2016.
 */
interface Geocoder {
    Point geocode(String address, String city, int zipcode)
}
