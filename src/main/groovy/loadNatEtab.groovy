import ddi.loader.neo4j.GraphDb

/**
 * Created by batman on 04/12/2016.
 */

// Db
def db = new GraphDb('neo4j', 'sirene')

//
def sourceFolder = '/Users/batman/Documents/insee'
def natEtab = "$sourceFolder/natetab_selection.csv"

def query = 'create (:NatEtab {libelle: {libelle}, code: {code}})'

def f = new File(natEtab)
def session = db.connect()
def tx = session.beginTransaction()

println "Traitement de NatEtab..."
f.eachLine {
    def fields = it.split(',')
    tx.run(query, [
            'libelle'       : fields[1],
            'code': fields[0]
    ])
}

tx.success()

session.close()