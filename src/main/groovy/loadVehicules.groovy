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
def db = new GraphDb('neo4j', 'ddi')

def queryImmat = 'match (e:Entreprise {siren: {siren}}) ' +
        'with e ' +
        'create (v:Vehicule {' +
        'immat: {immat},' +
        'premiereCirculation: {date},' +
        'co2: {co2},' +
        'neuf: {neuf}' +
        '}) ' +
        'create (e)-[:POSSEDE]->(v)'

def queryMarque = 'merge (m:Marque {nom: {marque}}) ' +
        'with m ' +
        'match (v:Vehicule {immat: {immat}}) ' +
        'create (v)-[:EST_UNE]->(m)'

def queryTopGenre = 'merge (g:TopGenre {nom: {topGenre}}) ' +
        'with g ' +
        'match (v:Vehicule {immat: {immat}}) ' +
        'create (v)-[:A_TOPGENRE]->(g)'

def queryEnergie = 'merge (e:Energie {nom: {energie}}) ' +
        'with e ' +
        'match (v:Vehicule {immat: {immat}}) ' +
        'create (v)-[:ROULE_AU]->(e)'

def queryEtablissementParc = 'match (t:Etablissement)<-[:GERE]-(:Entreprise {siren: {siren}}) ' +
        'match (g:TopGenre {nom: {topGenre}}) ' +
        'match (e:Energie {nom: {energie}}) ' +
        'match (m:Marque  {nom: {marque}}) ' +
        'merge (t)-[:POSSEDE]->(g) ' +
        'merge (t)-[:POSSEDE]->(e) ' +
        'merge (t)-[:POSSEDE]->(m) '

db.load(inputFile) { Transaction tx, Map<String, String> record ->
    String co2Value = record['IM_TXCO2']
    boolean isNeuf = record['IM_NEUFOCCAS'] == 'N'

    tx.run(queryImmat, [
            'immat': record['IM_NUMIMMAT'],
            'date' : dateConverter.convertDate(record['IM_DAT1MCIR']),
            'co2'  : co2Value ? Integer.parseInt(co2Value) : null,
            'neuf' : isNeuf,
            'siren': Long.parseLong(record['SIREN'])
    ])

    tx.run(queryEnergie, [
            'energie': record['IM_CDTYPENE'],
            'immat'  : record['IM_NUMIMMAT']
    ])

    tx.run(queryMarque, [
            'marque': record['IM_CDMARQUE'],
            'immat' : record['IM_NUMIMMAT']
    ])

    tx.run(queryTopGenre, [
            'topGenre': record['IM_CDTOPGENRE'],
            'immat'   : record['IM_NUMIMMAT']
    ])

    tx.run(queryEtablissementParc, [
            'siren': Long.parseLong(record['SIREN']),
            'topGenre': record['IM_CDTOPGENRE'],
            'energie': record['IM_CDTYPENE'],
            'marque': record['IM_CDMARQUE']
    ])
}
