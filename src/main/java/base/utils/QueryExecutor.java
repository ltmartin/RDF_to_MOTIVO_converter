package base.utils;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.springframework.util.Assert;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.RecursiveTask;
import java.util.logging.Level;
import java.util.logging.Logger;

public class QueryExecutor extends RecursiveTask<Set<Triple>> {
    private final Logger logger = Logger.getLogger(QueryExecutor.class.getName());

    public static final String COUNT_VAR_NAME = "?count";

    private String endpoint;
    private String query;
    private Set<Triple> results;
    private Boolean runQuery = false;
    private String datasetIri;
    private Integer amountOfTriplesInDataset;
    private LinkedList<QueryExecutor> executors;


    public QueryExecutor(String endpoint, String query, String datasetIri, Integer amountOfTriplesInDataset) {
        this.endpoint = endpoint;
        this.query = query;
        this.datasetIri = datasetIri;
        this.amountOfTriplesInDataset = amountOfTriplesInDataset;
        executors = new LinkedList<>();
    }

    public Set<Triple> runSelectQuery() {
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
        } catch (QueryParseException e) {
            System.out.println("===============================================");
            logger.log(Level.SEVERE, "Error processing the query: \n" + query + "\n");
            System.out.println("===============================================");
        }
        return results;
    }

    public Integer runCountQuery() {
        Assert.notNull(query, "The query must be set in advance.");
        Integer countResult = 0;

        try (QueryExecution qexec = QueryExecutionFactory.sparqlService(endpoint, query)) {
            ResultSet rs = qexec.execSelect();
            while (rs.hasNext()) {
                QuerySolution row = rs.next();
                countResult = row.get(COUNT_VAR_NAME).asLiteral().getInt();
            }
        } catch (QueryParseException e) {
            System.out.println("===============================================");
            logger.log(Level.SEVERE, "Error processing the query: \n" + query + "\n");
            System.out.println("===============================================");
        }

        return countResult;
    }

    public Set<Triple> getResults() {
        return results;
    }

    public void setRunQuery(Boolean runQuery) {
        this.runQuery = runQuery;
    }


    @Override
    protected Set<Triple> compute() {
        if (runQuery)
            runSelectQuery();
        else {

            final Integer cpuCount = Runtime.getRuntime().availableProcessors();
            final Integer triplesPerCpu = amountOfTriplesInDataset / cpuCount;

            for (int i = 0; i < cpuCount - 1; i++) {
                String query = "SELECT DISTINCT ?s ?p ?o FROM <" + datasetIri + "> WHERE {?s ?p ?o} LIMIT " + triplesPerCpu.toString() + " OFFSET " + i * triplesPerCpu;
                QueryExecutor queryExecutor = new QueryExecutor(endpoint, query, datasetIri, amountOfTriplesInDataset);
                queryExecutor.setRunQuery(true);
                executors.add(queryExecutor);
            }

            String query = "SELECT DISTINCT ?s ?p ?o FROM <" + datasetIri + "> WHERE {?s ?p ?o} LIMIT " + amountOfTriplesInDataset + " OFFSET " + ((cpuCount - 1) * triplesPerCpu);
            QueryExecutor queryExecutor = new QueryExecutor(endpoint, query, datasetIri, amountOfTriplesInDataset);
            queryExecutor.setRunQuery(true);
            executors.add(queryExecutor);

            for (int i = 0; i < executors.size() - 1; i++) {
                executors.get(i).fork();
            }
            results = executors.get(executors.size() - 1).compute();

            for (int i = 0; i < executors.size() - 1; i++) {
                results.addAll(executors.get(i).join());
            }
        }
        return results;
    }
}
