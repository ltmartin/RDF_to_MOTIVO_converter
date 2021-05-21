package base.utils;

import base.Application;
import org.apache.jena.graph.Triple;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.Set;

@SpringBootTest(classes = Application.class)
class QueryExecutorTest {

    private QueryExecutor queryExecutor;
    @Value("${sparql.endpoint}")
    private String endpoint;
    @Value("${sparql.dataset_IRI}")
    private String datasetIri;

    @Test
    void run() {
        String query = "SELECT DISTINCT ?s ?p ?o WHERE {?s ?p ?o}";
        queryExecutor = new QueryExecutor(endpoint, query);
        queryExecutor.start();

        while (queryExecutor.isAlive()){}

        Set<Triple> results = queryExecutor.getResults();

        Assert.notEmpty(results, "The collection is empty.");
    }

    @Test
    void runCountQuery() {
        final String triplesCountQuery = "SELECT (count(*) AS "+ QueryExecutor.COUNT_VAR_NAME +") FROM <"+ datasetIri + "> WHERE {?s ?p ?o}";
        queryExecutor = new QueryExecutor(endpoint, triplesCountQuery);
        Integer result = queryExecutor.runCountQuery();
        Assert.notNull(result, "Count query went wrong.");
    }
}