package ddi.loader.neo4j

import ddi.loader.CsvHeader
import org.neo4j.driver.v1.AuthTokens
import org.neo4j.driver.v1.Driver
import org.neo4j.driver.v1.GraphDatabase
import org.neo4j.driver.v1.Session

/**
 * Created by batman on 26/10/2016.
 */
class GraphDb {

    private Driver driver
    private Session session
    private CsvHeader csvHeader
    private File inputFile
    private Neo4jAuth auth

    GraphDb(String user, String pwd, File f) {
        this.inputFile = f
        this.auth = [user, pwd] as Neo4jAuth
    }

    private Session connect() {
        if (session) {
            return session
        }

        driver = GraphDatabase.driver("bolt://localhost", AuthTokens.basic(auth.user, auth.pwd))
        session = driver.session()

        return session
    }

    void close() {
        session.close()
        driver.close()
    }

    void load(Closure fn) {
        println "Connection to the DB"
        def session = connect()

        println "Open the file ${inputFile.absolutePath}"
        def reader = inputFile.newReader()

        // jumps the header
        println "Reads the header"
        csvHeader = new CsvHeader(reader.readLine())

        long totalDuration = 0
        def tx = session.beginTransaction()
        try {
            int count = 0

            println "Reading..."

            long startBlock = System.currentTimeMillis()


            reader.eachLine {
                ++count



                if (count % 10_000 == 0) {
                    def duration = System.currentTimeMillis() - startBlock
                    println "$count records (${duration / 1000} s)"
                    startBlock = System.currentTimeMillis()
                    totalDuration += duration
                }

                if (count % 1000 == 0) {
                    tx.success()
                    tx.close()
                    tx = session.beginTransaction()
                }

                def fields = it.split(';', -1)

                fn(tx, csvHeader.values(fields))
            }

            tx.success()
            tx.close()
        } catch (Exception e) {
            e.printStackTrace()
            tx.failure()
            tx.close()
        } finally {
            println "Close the DB connection"
            close()
            println "File processing duration: ${totalDuration / 1000} s"
        }
    }
}
