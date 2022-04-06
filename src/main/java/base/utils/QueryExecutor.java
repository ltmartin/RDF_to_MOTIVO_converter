package base.utils;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.*;
import org.springframework.util.Assert;

import java.net.ConnectException;
import java.util.*;
import java.util.concurrent.RecursiveTask;
import java.util.concurrent.TimeUnit;
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
    private Queue<QueryExecutor> executors;


    public QueryExecutor(String endpoint, String query, String datasetIri, Integer amountOfTriplesInDataset) {
        this.endpoint = endpoint;
        this.query = query;
        this.datasetIri = datasetIri;
        this.amountOfTriplesInDataset = amountOfTriplesInDataset;
        executors = new LinkedList<>();
    }

    public Set<Triple> runSelectQuery() {
        Assert.notNull(query, "The query must be set in advance.");
        logger.log(Level.ALL, "Processing query: " + query);
        System.out.println("Processing query: " + query);
        results = new HashSet<>();
        boolean waitForConnection = false;
        do {
            try (QueryExecution qexec = QueryExecutionFactory.sparqlService(endpoint, query)) {
                qexec.setTimeout(5, TimeUnit.DAYS);
                ResultSet rs = qexec.execSelect();
                logger.log(Level.ALL, "Query results obtained.");
                while (rs.hasNext()) {
                    QuerySolution row = rs.next();
                    Node subject = row.get("?s").asNode();
                    Node predicate = row.get("?p").asNode();
                    Node object = row.get("?o").asNode();
                    Triple triple = new Triple(subject, predicate, object);
                    results.add(triple);
                }
                logger.log(Level.ALL, "Query results processed.");
                System.out.println("Query results processed.");
                waitForConnection = false;
            } catch (QueryParseException e) {
                System.out.println("===============================================");
                logger.log(Level.SEVERE, "Error processing the query: \n" + query + "\n");
                System.out.println("===============================================");
                waitForConnection = false;
            } catch (Exception e) {
                waitForConnection = true;
                System.out.println("===============================================");
                logger.log(Level.INFO, "Waiting to connect to Virtuoso");
                System.out.println("===============================================");
            }
        } while (waitForConnection);

        return results;
    }

    public Integer runCountQuery() {
        Assert.notNull(query, "The query must be set in advance.");
        Integer countResult = 0;

        boolean waitForConnection = false;
        do {
            try (QueryExecution qexec = QueryExecutionFactory.sparqlService(endpoint, query)) {
                ResultSet rs = qexec.execSelect();
                while (rs.hasNext()) {
                    QuerySolution row = rs.next();
                    countResult = row.get(COUNT_VAR_NAME).asLiteral().getInt();
                }
                waitForConnection = false;
            } catch (QueryParseException e) {
                System.out.println("===============================================");
                logger.log(Level.SEVERE, "Error processing the query: \n" + query + "\n");
                System.out.println("===============================================");
                waitForConnection = false;
            } catch (Exception e) {
                waitForConnection = true;
                System.out.println("===============================================");
                logger.log(Level.INFO, "Waiting to connect to Virtuoso");
                System.out.println("===============================================");
            }
        } while (waitForConnection);
        System.out.println("Amount of triples: " + countResult);
        logger.log(Level.ALL, "Amount of triples: " + countResult);
        return countResult;
    }

    public void setRunQuery(Boolean runQuery) {
        this.runQuery = runQuery;
    }


    @Override
    protected Set<Triple> compute() {
        System.out.println("Compute method.");
        logger.log(Level.ALL, "Compute method.");
        Integer numberOfThreads = 32;
        if (runQuery)
            runSelectQuery();
        else {

            final Integer triplesPerQuery = 5000;
            Integer triplesRetrieved = 0;

            for (int i = 0; triplesRetrieved < amountOfTriplesInDataset; i++) {
                String query = "SELECT ?s ?p ?o FROM <" + datasetIri + "> WHERE {?s ?p ?o} LIMIT " + triplesPerQuery.toString() + " OFFSET " + i * triplesPerQuery;
                triplesRetrieved += 5000;
                QueryExecutor queryExecutor = new QueryExecutor(endpoint, query, datasetIri, amountOfTriplesInDataset);
                queryExecutor.setRunQuery(true);
                executors.add(queryExecutor);
            }

            while (!executors.isEmpty()) {
                List<QueryExecutor> forkedExecutors = new LinkedList<>();
                for (int i = 0; (executors.size() > 1) && (i < numberOfThreads - 1); i++) {
                    QueryExecutor executor = executors.poll();
                    executor.fork();
                    forkedExecutors.add(executor);
                }
                results = executors.poll().compute();

                for (int i = 0; i < forkedExecutors.size(); i++) {
                    results.addAll(forkedExecutors.get(i).join());
                }
            }
        }
        return (results != null)? results : new HashSet<>();
    }
}
