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
        'premiereCirculation: {date},' +
        'co2: {co2},' +
        'neuf: {neuf}' +
        '}) ' +
        'with v ' +
        'match (e:Entreprise {siren: {siren}}) ' +
        'create (e)-[:POSSEDE]->(v)'

//TODO Query for brand

//TODO Query for sort of vehicle

//TODO Query for sort of energy


int id = 0
db.load { Transaction tx, Map<String, String> record ->
    ++id

    tx.run(queryImmat, Values.parameters(
            'siren', Long.parseLong(record['SIREN']),
            'id', id,
            'name', null,
            'immat', record['IM_NUMIMMAT'],
            'date', dateConverter.convertDate(record['IM_DAT1MCIR']),
            'co2', Integer.parseInt(record['IM_TXCO2']),
            'neuf', record['IM_NEUFOCCAS'] == 'N'
    ))

}
