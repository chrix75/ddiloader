package geocoding

/**
 * Created by batman on 11/11/2016.
 */
class FakeGeocoder implements Geocoder {
    @Override
    Point geocode(String address, String city, int zipcode) {
        return new Point(latitude: 1.0, longitude: 1.0)
    }
}
