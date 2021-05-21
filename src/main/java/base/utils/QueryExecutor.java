package base.utils;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.springframework.util.Assert;

import java.util.HashSet;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QueryExecutor extends Thread {
    private final Logger logger = Logger.getLogger(QueryExecutor.class.getName());

    public static final String COUNT_VAR_NAME = "?count";

    private String endpoint;
    private String query;
    private Set<Triple> results;

    public QueryExecutor(String endpoint, String query) {
        this.endpoint = endpoint;
        this.query = query;
    }

    public void run(){
        this.runSelectQuery();
    }

    public Set<Triple> runSelectQuery(){
        Assert.notNull(query, "The query must be set in advance.");
        results = new HashSet<>();
        try (QueryExecution qexec = QueryExecutionFactory.sparqlService(endpoint, query)) {
            ResultSet rs = qexec.execSelect();
            while (rs.hasNext()) {
                QuerySolution row = rs.next();
                Node subject = row.get("?s").asNode();
                Node predicate = row.get("?p").asNode();
                Node object = row.get("?o").asNode();
                Triple triple = new Triple(subject, predicate, object);
                results.add(triple);
            }
        } catch (QueryParseException e){
            System.out.println("===============================================");
            logger.log(Level.SEVERE, "Error processing the query: \n" + query + "\n");
            System.out.println("===============================================");
        }
        return results;
    }

    public Integer runCountQuery(){
        Assert.notNull(query, "The query must be set in advance.");
        Integer countResult = 0;

        try (QueryExecution qexec = QueryExecutionFactory.sparqlService(endpoint, query)) {
            ResultSet rs = qexec.execSelect();
            while (rs.hasNext()) {
                QuerySolution row = rs.next();
                countResult = row.get(COUNT_VAR_NAME).asLiteral().getInt();
            }
        } catch (QueryParseException e){
            System.out.println("===============================================");
            logger.log(Level.SEVERE, "Error processing the query: \n" + query + "\n");
            System.out.println("===============================================");
        }

        return countResult;
    }

    public Set<Triple> getResults() {
        return results;
    }

}
