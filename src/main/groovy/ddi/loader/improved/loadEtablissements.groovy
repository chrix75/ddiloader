package ddi.loader.improved

import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.Transaction
import org.neo4j.driver.v1.Values

/**
 * Charge un fichier d'établissements.
 *
 * Ce script prend en argument le chemin du fichier à charger.
 * Created by batman on 26/10/2016.
 */

def header = 'SIRET;SIREN;NIC;ICODE;IPMOR;NOMMEN_APEN700;APEN700;APEN3;APEN2;CJ;CJ2;NOMEN;SIGLE;SIEGE;NNUMVOIE;TYPVOIE;' +
        'LIBVOIE;NOMMEN_APET700;APET700;APET3;APET2;IPAYS;RPET;TEFET;TEFEN;CODPOS;DPT;L1_NOMEN;L2_COMP;CEDEX;ACHEM;' +
        'LIBCOM;ENSEIGNE;ITEL;TEL_GEN;IFAX;FAX_GEN;CRITERE;L3_CADR;L4_ADR;IDMAJINS;NB_CONTACT;IND_CHAINAGE;MOTRECH;' +
        'PA_NBIMMAT'

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
        'fax: {fax},' +
        'nbImmat: {nbImmat}' +
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

def queryEntreprise = 'merge (n:Entreprise { siren: {siren} }) ' +
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
db.load { Transaction tx, Map<String, String> record ->
    ++id

    tx.run(queryEtab,
           Values.parameters(
                   "siret", Long.parseLong(record["SIRET"]),
                   "rs", record["L1_NOMEN"],
                   "rs2", record["L2_COMP"],
                   "rs3", record["ENSEIGNE"],
                   "siege", record["SIEGE"] == '1',
                   "adresse", record["L4_ADR"],
                   "batiment", record["L3_CADR"],
                   "motRech", record["MOTRECH"],
                   "telephone", record["TEL_GEN"],
                   "fax", record["FAX_GEN"],
                   "nbImmat", Integer.parseInt(record["PA_NBIMMAT"] ? record["PA_NBIMMAT"] : '0'),
                   "id", id,
                   "name", null
           ))

    tx.run(queryGeo,
           Values.parameters(
                   'region', Integer.parseInt(record['RPET']),
                   'dpt', Integer.parseInt(record['DPT']),
                   'codePostal', Integer.parseInt(record['CODPOS']),
                   'ville', record['ACHEM'],
                   'siret', Long.parseLong(record['SIRET'])
           ))

    tx.run(queryApet,
           Values.parameters(
                   'code700', record['APET700'],
                   'code3', record['APET3'],
                   'code2', record['APET2'],
                   'siret', Long.parseLong(record['SIRET'])
           ))

    tx.run(queryEntreprise,
           Values.parameters(
                   'siren', Long.parseLong(record['SIREN']),
                   'siret', Long.parseLong(record['SIRET'])
           ))

    tx.run(queryApen,
           Values.parameters(
                   'code700', record['APEN700'],
                   'code3', record['APEN3'],
                   'code2', record['APEN2'],
                   'siren', Long.parseLong(record['SIREN'])
           ))
}


