package ddi.loader

import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.Values

/**
 * Created by batman on 23/10/2016.
 */

def loader = new ImmatLoader()

def f = new File('/Volumes/Comics01/test_neo4j/ddi/raw_data/immat.csv')

def query = "match (e:Entreprise {siren: {siren}}) " +
        "with e " +
        "create (e)-[:POSSEDE]->(:Vehicule {immat: {immat}, dateCarteGrise: {dateCarteGrise}, " +
        "datePremiereCirculation: {date1mcir}, neuf: {neufFlag}, marque: {marque}, modele: {modele}," +
        "gamme: {gamme}, topGenre: {topGenre}, genre: {genre}, typeLocation: {typeLocation}," +
        "energie: {energie}, typeCarrosserie: {typeCarrosserie}, typeCarrosserieCG: {typeCarrosserieCG}," +
        "co2: {co2} })"


def driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "ddi"));

def start = System.currentTimeMillis()

def startBlock = System.currentTimeMillis()
def long endBlock
def session = driver.session()

int counter = 0
def tx = session.beginTransaction()
f.eachLine {

    ++counter
/*

    if (counter % 1000 == 0) {
        tx.success()
        println "Closing..."
        //session.close()
        //session = driver.session()
        tx = session.beginTransaction()
    }
*/

    if (counter % 5_000 == 0) {
        tx.success()
        endBlock = System.currentTimeMillis()
        println "Writes: $counter (duration: ${(endBlock - startBlock) / 1000} s"
        startBlock = endBlock
        tx.close()
        tx = session.beginTransaction()
    }

    if (counter > 1) {
        def fields = it.split(/\|/, -1)
        def values = loader.values(fields)


        tx.run(query,
                    Values.parameters(
                            'id', counter,
                            'name', '',
                            'siren', Long.parseLong(values['IM_SIRENCCD']),
                            'immat', values['IM_NUMIMMAT'],
                            'dateCarteGrise', Integer.parseInt(values['IM_DATCARTGRIS']),
                            'date1mcir', Integer.parseInt(values['IM_DAT1MCIR']),
                            'neufFlag', values['IM_NEUFOCCAS'] == 'O',
                            'marque', values['IM_CDMARQUE'],
                            'modele', values['IM_CDMODELE'],
                            'gamme', values['IM_CDGAMME'],
                            'topGenre', values['IM_CDTOPGENRE'],
                            'genre', values['IM_CDGENRE'],
                            'typeLocation', values['IM_CDTYPLOC'],
                            'energie', values['IM_CDTYPENE'],
                            'typeCarrosserie', values['IM_CDTYPCAR'],
                            'typeCarrosserieCG', values['IM_CDTYPCARCG'],
                            'co2', values['IM_TXCO2'] ? Integer.parseInt(values['IM_TXCO2']) : null
                    ))

        //session.close()

        values = null
        fields = null
        //session = null
    }
}

tx.success()
tx.close()
println "Closing..."
session.close()
driver.close()
println "Closed"

def end = System.currentTimeMillis()
def duration = (end - start) / 1000
println "Duration: $duration"
