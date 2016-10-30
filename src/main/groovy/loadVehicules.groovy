import ddi.loader.DateConverter
import ddi.loader.neo4j.GraphDb
import org.neo4j.driver.v1.Transaction
import org.neo4j.driver.v1.Values

/**
 * Chargement d'un fichier d'immatriculiations.
 *
 * Ce script prend en argument le chemin du fichier à charger.
 *
 * Created by batman on 28/10/2016.
 */

if (args.length == 0) {
    System.err.println("Input file argument missing")
    System.exit(1)
}

def inputFile = new File(args[0])
if (!inputFile.exists()) {
    System.err.println("Input file $inputFile not found")
    System.exit(1)
}

def dateConverter = new DateConverter()

// Db
def db = new GraphDb('neo4j', 'ddi', inputFile)

def queryImmat = 'create (v:Vehicule {' +
        'id: {id},' +
        'name: {name},' +
        'immat: {immat},' +
        'topGenre: {topGenre},' +
        'energie: {energie},' +
        'marque: {marque},' +
        'premiereCirculation: {date},' +
        'co2: {co2},' +
        'neuf: {neuf}' +
        '}) ' +
        'with v ' +
        'match (e:Entreprise {siren: {siren}}) ' +
        'create (e)-[:POSSEDE]->(v)'

int id = 0
def params = [:]
db.load { Transaction tx, Map<String, String> record ->
    ++id

    String co2Value = record['IM_TXCO2']

    params['siren'] = Long.parseLong(record['SIREN'])
    params['id'] = id
    params['name'] = null
    params['topGenre'] = record['IM_CDTOPGENRE']
    params['marque'] = record['IM_CDMARQUE']
    params['energie'] = record['IM_CDTYPENE']
    params['immat'] = record['IM_NUMIMMAT']
    params['date'] = dateConverter.convertDate(record['IM_DAT1MCIR'])
    params['co2'] = co2Value ? Integer.parseInt(co2Value) : null
    params['neuf'] = record['IM_NEUFOCCAS'] == 'N'

    tx.run(queryImmat, params)
}
