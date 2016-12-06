import ddi.loader.neo4j.GraphDb

/**
 * Created by batman on 04/12/2016.
 */

// Db
def db = new GraphDb('neo4j', 'sirene')

//
def sourceFolder = '/Users/batman/Documents/insee'
def nj = "$sourceFolder/nj_selection.csv"

def njQuery = 'create (:Nj {libelle: {libelle}, code: {code}})'

def fn = new File(nj)
def session = db.connect()
def tx = session.beginTransaction()

println "Traitement de NJ..."
fn.eachLine {
    def fields = it.split(',')
    tx.run(njQuery, [
            'libelle'       : fields[1],
            'code': fields[0]
    ])
}

tx.success()

session.close()