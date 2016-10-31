import ddi.loader.EtablissementsLoader
import groovyx.gpars.GParsPool
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


def files = args.collect {
    def f = new File(it)
    if (!f.exists()) {
        System.err.println("Input file $f not found")
        System.exit(1)
    }

    return f
}

def loader = new EtablissementsLoader()

GParsPool.withPool(2) {
    files.eachParallel { File f ->
        loader.loadFile(f)
    }
}


