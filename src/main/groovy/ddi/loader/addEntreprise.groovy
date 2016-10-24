package ddi.loader

import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.Record
import org.neo4j.driver.v1.Session
import org.neo4j.driver.v1.Values

/**
 * Created by batman on 23/10/2016.
 */

def eachResult(Session session, String query, Closure fn) {
    def etabResult = session.run(query)
    while (etabResult.hasNext()) {
        def record = etabResult.next()
        fn(record)
    }

}

def driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic("neo4j", "ddi"));
def session = driver.session()

def matchEtabQuery = "match (e:Etablissement) return e.siret"
def entrepriseQuery = "match (e:Etablissement {siret: {siret}}) " +
        "merge (n:Entreprise {siren: {siren}}) " +
        "with e, n " +
        "merge (e)<-[:EST_SIEGE]-(n)"

int count = 0

def start = System.currentTimeMillis()

eachResult(session, matchEtabQuery) { Record record ->
    if (++count % 10000 == 0) {
        println "$count etablissements processed"
    }

    long siret = record.get('e.siret').asLong()
    long siren =  siret / 100000

    session.run(entrepriseQuery, Values.parameters("siret", siret, "siren", siren))
}

session.close()
driver.close()

def end = System.currentTimeMillis()
def duration = (end - start) / 1000
println "Duration: $duration"



