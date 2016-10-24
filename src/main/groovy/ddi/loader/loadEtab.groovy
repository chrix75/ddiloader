package ddi.loader

import groovyx.gpars.actor.DefaultActor
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.Session
import org.neo4j.driver.v1.Values

import java.sql.Connection
import java.sql.DriverManager
import java.util.concurrent.LinkedBlockingQueue

/**
 * Created by batman on 23/10/2016.
 */

def driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "ddi"));

class Writer extends DefaultActor {
    Session session
    String query

    Writer(Session session, String query) {
        this.session = session
        this.query = query
    }

    @Override
    protected void act() {
        loop {
            react { Map<String, String> values ->
                if (values.size() == 0) {
                    println "Closing session $this"
                    session.close()
                    println "Terminate thread $this"
                    terminate()
                } else {
                    session.run(query,
                                 Values.parameters("siret", Long.parseLong(values['SIRET']),
                                                   "rs", values['RAISON_SOCIALE'],
                                                   "rs2", values['RAISON_SOCIALE2'],
                                                   "addressExt", values['LIBELLE_BATIMENT'],
                                                   "address", values['LIBELLE_ADRESSE'],
                                                   "zipcode", Integer.parseInt(values['CODE_POSTAL']),
                                                   "cci", Integer.parseInt(values['CODE_COMMUNE']),
                                                   "city", values['CENTRE_DISTRIBUTEUR']))

                }
            }
        }
    }
}

def query = "create (:Etablissement {siret: {siret}, raisonSociale: {rs}, raisonSociale2: {rs2}, " +
        "batiment: {addressExt}, adresse: {address}, codePostal: {zipcode}, cci:{cci}, ville: {city}})"


def actors = [] as List<Writer>
10.times {
    def writer = new Writer(driver.session(), query)
    actors << writer
    writer.start()
}

def f = new File('/Volumes/Comics01/test_neo4j/ddi/raw_data/etablissements.csv')
def loader = new EtablissementLoader()

int counter = 0

def start = System.currentTimeMillis()

def session = driver.session()

println "Reading the file..."

f.eachLine {
    ++counter

    if (counter > 1) {
        def fields = it.split(/\|/, -1)
        def values = loader.values(fields)

        if (counter % 8 == 0) {
            session.run(query,
                        Values.parameters("siret", Long.parseLong(values['SIRET']),
                                          "rs", values['RAISON_SOCIALE'],
                                          "rs2", values['RAISON_SOCIALE2'],
                                          "addressExt", values['LIBELLE_BATIMENT'],
                                          "address", values['LIBELLE_ADRESSE'],
                                          "zipcode", Integer.parseInt(values['CODE_POSTAL']),
                                          "cci", Integer.parseInt(values['CODE_COMMUNE']),
                                          "city", values['CENTRE_DISTRIBUTEUR']))


        } else {
            int writerNum = counter % actors.size()
            actors[writerNum] << values
        }

        if (counter % 10_000 == 0) {
            println "Writes: $counter"
        }
    }
}

println "File completely read"

actors*.send([:])
actors*.join()

driver.close()


def end = System.currentTimeMillis()
def duration = (end - start) / 1000
println "Duration: $duration"