import ddi.loader.neo4j.GraphDb

/**
 * Created by batman on 04/12/2016.
 */

// Db
def db = new GraphDb('neo4j', 'sirene')

//
def sourceFolder = '/Users/batman/Documents/insee'
def ape700 = "$sourceFolder/ape700.csv"

def ape700Query = 'create (:Ape700 {libelle: {libelle}, code: {code}})'

def f = new File(ape700)
def session = db.connect()
def tx = session.beginTransaction()

println "Traitement de APE700..."
f.eachLine {
    def fields = it.split(',')
    tx.run(ape700Query, [
            'libelle'       : fields[1],
            'code': fields[0]
    ])
}

tx.success()

session.close()