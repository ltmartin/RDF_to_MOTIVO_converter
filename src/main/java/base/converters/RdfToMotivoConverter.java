package base.converters;

import base.utils.QueryExecutor;
import org.apache.jena.graph.Triple;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Set;
import java.util.concurrent.*;

@Component
@Lazy
public class RdfToMotivoConverter {

    @Value("${sparql.endpoint}")
    private String endpoint;
    @Value("${sparql.dataset_IRI}")
    private String datasetIri;

    private ConcurrentMap<String, String> replacementsMap;

    public RdfToMotivoConverter() {
        replacementsMap = new ConcurrentHashMap();
    }

    public void convert(){
        final String triplesCountQuery = "SELECT (count(*) AS "+ QueryExecutor.COUNT_VAR_NAME +") FROM <"+ datasetIri + "> WHERE {?s ?p ?o}";
        final QueryExecutor countQueryExecutor = new QueryExecutor(endpoint, triplesCountQuery);
        final Integer amountOfTriplesInDataset = countQueryExecutor.runCountQuery();
        final Integer cpuCount = Runtime.getRuntime().availableProcessors();
        final Integer triplesPerCpu = amountOfTriplesInDataset / cpuCount;


        for (int i = 0; i < cpuCount; i++) {
            String query = "SELECT DISTINCT ?s ?p ?o FROM <"+ datasetIri + "> WHERE {?s ?p ?o} LIMIT " + triplesPerCpu.toString() + " OFFSET " + String.valueOf(i * triplesPerCpu) ;
            CompletableFuture<Set<Triple>> triples = getTriples(query);


        }
    }

    private CompletableFuture<Set<Triple>> getTriples(String query){
        CompletableFuture<Set<Triple>> triples = new CompletableFuture<>();

        Executors.newCachedThreadPool().submit(() -> {
            QueryExecutor queryExecutor = new QueryExecutor(endpoint, query);
            triples.complete(queryExecutor.runSelectQuery());
            return null;
        });

        return triples;
    }

}
