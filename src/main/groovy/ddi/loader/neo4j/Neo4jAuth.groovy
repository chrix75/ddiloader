package ddi.loader.neo4j

/**
 * Created by batman on 26/10/2016.
 */
class Neo4jAuth {
    String user
    String pwd

    Neo4jAuth(String user, String pwd) {
        this.user = user
        this.pwd = pwd
    }
}
