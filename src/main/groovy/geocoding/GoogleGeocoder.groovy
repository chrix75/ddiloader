package geocoding

import groovyx.net.http.RESTClient

/**
 * Created by batman on 11/11/2016.
 */
class GoogleGeocoder implements Geocoder {
    private RESTClient client

    GoogleGeocoder() {
        client = new RESTClient(
                new URI('https://maps.googleapis.com'),
                'application/json'
        )
    }

    @Override
    Point geocode(String address, String city, int zipcode) {
        def fullAddress = "$address $zipcode $city france"
        def addressForGeocode = fullAddress.split().join('+')

        def path = '/maps/api/geocode/json'
        def params = ['address': addressForGeocode,
                      'key'    : 'AIzaSyAUnriZ0o2jF5ZtRzuxQlvoIlvcr0hAXRg']

        try {
            def resp = client.get(path: path, query: params)

            def status = resp.getStatus()
            if (status != 200) {
                throw new RuntimeException("Error status code")
            }

            def data = resp.getData()
            def quality = data.results.geometry.location_type.size() > 0 ? data.results.geometry.location_type[0] : "UNKNOWN"

            if (quality == 'ROOFTOP'
                    || quality == 'RANGE_INTERPOLATED'
                    || quality == 'GEOMETRIC_CENTER') {

                def lat = data.results.geometry.location.lat[0]
                def lng = data.results.geometry.location.lng[0]

                return new Point(latitude: lat, longitude: lng)
            }
        } catch (Exception e) {
            println "Address failure: [$fullAddress]"
            return null
        }

    }
}
