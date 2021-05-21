package base.utils;

import base.Application;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.Assert;

@SpringBootTest(classes = Application.class)
class QueryExecutorTest {

    private QueryExecutor queryExecutor;
    @Value("${sparql.endpoint}")
    private String endpoint;
    @Value("${sparql.dataset_IRI}")
    private String datasetIri;

    @Test
    void runCountQuery() {
        final String triplesCountQuery = "SELECT (count(*) AS "+ QueryExecutor.COUNT_VAR_NAME +") FROM <"+ datasetIri + "> WHERE {?s ?p ?o}";
        queryExecutor = new QueryExecutor(endpoint, triplesCountQuery, datasetIri, 0);
        Integer result = queryExecutor.runCountQuery();
        Assert.notNull(result, "Count query went wrong.");
    }
}