import ddi.loader.neo4j.GraphDb
import org.neo4j.driver.v1.Transaction

/**
 * Charge un fichier d'établissements.
 *
 * Ce script prend en argument le chemin du fichier à charger.
 *
 * Created by batman on 26/10/2016.
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

// Db
def db = new GraphDb('neo4j', 'ddi', inputFile)

def queryEtab = 'create (:Etablissement {siret: {siret}, ' +
        'rs: {rs}, ' +
        'rs2: {rs2},' +
        'rs3: {rs3},' +
        'siege: {siege}, ' +
        'adresse: {adresse},' +
        'batiment: {batiment},' +
        'motRech: {motRech},' +
        'telephone: {telephone}, ' +
        'fax: {fax}' +
        '})'

def queryGeo = 'merge (r:Region { region: {region} }) ' +
        'merge (d:Departement { departement: {dpt} }) ' +
        'merge (v:Ville { codePostal: {codePostal}, nom: {ville} }) ' +
        'with r, d, v ' +
        'match (e:Etablissement {siret: {siret} }) ' +
        'merge (e)-[:DOMICILIE]->(r) ' +
        'merge (e)-[:DOMICILIE]->(d) ' +
        'merge (e)-[:DOMICILIE]->(v)'

def queryApet = 'merge (apet700:Ape700 {code: {code700} }) ' +
        'merge (apet3:Ape3 {code: {code3} }) ' +
        'merge (apet2:Ape2 {code: {code2} }) ' +
        'with apet700, apet3, apet2 ' +
        'match (e:Etablissement {siret: {siret} }) ' +
        'merge (e)-[:COMME_ACTIVITE]->(apet700) ' +
        'merge (e)-[:COMME_ACTIVITE]->(apet3) ' +
        'merge (e)-[:COMME_ACTIVITE]->(apet2)'

def queryEntreprise = 'merge (n:Entreprise { siren: {siren}, nbImmat: {nbImmat} }) ' +
        'with n ' +
        'match (e:Etablissement { siret: {siret} })' +
        'merge (n)-[:GERE]->(e)'

def queryApen = 'merge (apen700:Ape700 {code: {code700} }) ' +
        'merge (apen3:Ape3 {code: {code3} }) ' +
        'merge (apen2:Ape2 {code: {code2} }) ' +
        'with apen700, apen3, apen2 ' +
        'match (e:Entreprise {siren: {siren} }) ' +
        'merge (e)-[:COMME_ACTIVITE]->(apen700) ' +
        'merge (e)-[:COMME_ACTIVITE]->(apen3) ' +
        'merge (e)-[:COMME_ACTIVITE]->(apen2)'

int id = 0
def etabParams = [:]
def geoParams = [:]
def apetParams = [:]
def apenParams = [:]
def entrParams = [:]

db.load { Transaction tx, Map<String, String> record ->
    ++id

    etabParams['siret'] = Long.parseLong(record['SIRET'])
    etabParams['rs'] = record['L1_NOMEN']
    etabParams['rs2'] = record['L2_COMP']
    etabParams['rs3'] = record['ENSEIGNE']
    etabParams['siege'] = record['SIEGE'] == '1'
    etabParams['adresse'] = record['L4_ADR']
    etabParams['batiment'] = record['L3_CADR']
    etabParams['motRech'] = record['MOTRECH']
    etabParams['telephone'] = record['TEL_GEN']
    etabParams['fax'] = record['FAX_GEN']
    etabParams['id'] = id
    etabParams['name'] = null
    tx.run(queryEtab, etabParams)


    geoParams['region'] = Integer.parseInt(record['RPET'])
    geoParams['dpt'] = Integer.parseInt(record['DPT'])
    geoParams['codePostal'] = Integer.parseInt(record['CODPOS'])
    geoParams['ville'] = record['ACHEM']
    geoParams['siret'] = Long.parseLong(record['SIRET'])
    tx.run(queryGeo, geoParams)

    apetParams['code700'] =  record['APET700']
    apetParams['code3'] =  record['APET3']
    apetParams['code2'] =  record['APET2']
    apetParams['siret'] =  Long.parseLong(record['SIRET'])
    tx.run(queryApet, apetParams)

    entrParams['siren'] =  Long.parseLong(record['SIREN'])
    entrParams['siret'] =  Long.parseLong(record['SIRET'])
    entrParams['nbImmat'] = Integer.parseInt(record['PA_NBIMMAT'] ? record['PA_NBIMMAT'] : '0')
    tx.run(queryEntreprise, entrParams)

    apenParams['code700'] =  record['APEN700']
    apenParams['code3'] =  record['APEN3']
    apenParams['code2'] =  record['APEN2']
    apenParams['siren'] =  Long.parseLong(record['SIREN'])
    tx.run(queryApen,apenParams)
}


