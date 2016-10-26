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

def query = ''

db.load { Transaction tx, Map<String, String> record ->
    tx.run(query,
           Values.parameters())

}


