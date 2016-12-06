import ddi.loader.neo4j.GraphDb

/**
 * Created by batman on 04/12/2016.
 */

// Db
def db = new GraphDb('neo4j', 'sirene')

//
def sourceFolder = '/Users/batman/Documents/insee'
def departements = "$sourceFolder/departements_selection.csv"
def regions = "$sourceFolder/regions_selection.csv"
def villes = "$sourceFolder/villes_selection.csv"

def villeQuery = 'create (:Ville {nom: {nom}, codePostal: {codePostal}, cci: {cci}})'

def fv = new File(villes)
def session = db.connect()
def tx = session.beginTransaction()

println "Traitement de villes..."
fv.eachLine {
    def fields = it.split(',')
    tx.run(villeQuery, [
            'nom'       : fields[3],
            'codePostal': fields[0],
            'cci'       : String.format("%s%s", fields[1], fields[2])
    ])
}

tx.success()

def departementQuery = 'create (:Departement {nom: {nom}, code: {code}})'

println "Traitement des departements"
def fd = new File(departements)
fd.eachLine {
    def fields = it.split(',')

    tx.run(departementQuery, [
            'nom': fields[1],
            'code': fields[0]
    ])
}

tx.success()

def regionQuery = 'create (:Region {nom: {nom}, code: {code}})'

println "Traitement des regions"
def fr = new File(regions)
fr.eachLine {
    def fields = it.split(',')

    tx.run(regionQuery, [
            'nom': fields[1],
            'code': fields[0]
    ])

}

tx.success()
session.close()