import ddi.loader.neo4j.GraphDb
import geocoding.FakeGeocoder
import geocoding.GoogleGeocoder
import org.neo4j.driver.internal.value.NodeValue

/**
 * Geocode les établissements de type Restauration rapide (5610C)
 * dans les arrondissements parisiens suivants: 12, 13, 20.
 *
 * Created by batman on 11/11/2016.
 */

// Db
def db = new GraphDb('neo4j', 'ddi')

def select = 'match (e:Etablissement)-[:DOMICILIE]-(v:Ville) ' +
        'match (o)-[:COMME_ACTIVITE]->(:Ape700 {code: "5610C"}) ' +
        'where e = o ' +
        'and v.codePostal in [75012, 75013, 75020] ' +
        'return e, v'

def update = 'match (e:Etablissement {siret: {siret}}) ' +
        'set e.latitude = {latitude}, ' +
        'e.longitude = {longitude}'

def session = db.connect()

def result = session.run(select)

def geocoder = new GoogleGeocoder()

def tx = session.beginTransaction()

result.each { record ->
    NodeValue etab = record.get('e')
    NodeValue city = record.get('v')

    def siret = etab.get('siret').asLong()
    def address = etab.get('adresse').asString()
    def cityname = city.get('nom').asString()
    def zipcode = city.get('codePostal').asInt()

    def coords = geocoder.geocode(address, cityname, zipcode)

    if (coords) {
        println "$siret (l: ${coords.latitude} L: ${coords.longitude})"

        tx.run(update, ['siret'    : siret,
                        'latitude' : coords.latitude,
                        'longitude': coords.longitude])
    }
}

tx.success()
tx.close()


session.close()
