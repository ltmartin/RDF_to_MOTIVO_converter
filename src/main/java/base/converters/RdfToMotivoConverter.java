package base.converters;

import base.utils.QueryExecutor;
import org.apache.commons.collections.set.SynchronizedSet;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;

@Component
@Lazy
public class RdfToMotivoConverter {
    private final static Charset ENCODING = StandardCharsets.UTF_8;
    @Value("${sparql.endpoint}")
    private String endpoint;
    @Value("${sparql.dataset_IRI}")
    private String datasetIri;

    private ConcurrentMap<String, Integer> replacementsMap;
    private ConcurrentMap<Integer, LinkedList<Integer>> connections;
    private Integer id = 0;
    private Integer numberOfEdges = 0;
    private Set<Triple> triples;

    public RdfToMotivoConverter() {
        replacementsMap = new ConcurrentHashMap();
        connections = new ConcurrentHashMap<>();
    }

    public void convert() {

        final String triplesCountQuery = "SELECT (count(*) AS " + QueryExecutor.COUNT_VAR_NAME + ") FROM <" + datasetIri + "> WHERE {?s ?p ?o}";
        final QueryExecutor countQueryExecutor = new QueryExecutor(endpoint, triplesCountQuery, datasetIri, 0);
        final Integer amountOfTriplesInDataset = countQueryExecutor.runCountQuery();

        final QueryExecutor queryExecutor = new QueryExecutor(endpoint, "",datasetIri, amountOfTriplesInDataset);
        triples = new ForkJoinPool().invoke(queryExecutor);

        triples.stream().forEach(triple -> {
            createReplacement(triple);
        });

        writeInputFile();
    }

    private void writeInputFile() {
        final String fileName = "input.txt";
        File file = new File(fileName);
        try {
            if (file.createNewFile()){
                BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, ENCODING));
                writer.write(id+1 + " " + numberOfEdges);
                writer.newLine();
                for (int i = 0; i < id; i++) {
                    List<Integer> adjacents = connections.get(i);
                    if ((null != adjacents) && (!adjacents.isEmpty())) {
                        writer.write(Integer.toString(i));
                        for (Integer adjacent : adjacents) {
                            writer.write(" " + Integer.toString(adjacent));
                        }
                        writer.newLine();
                    }
                }
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void createReplacement(Triple triple) {
        final String subject = triple.getSubject().toString();
        final Node object = triple.getObject();

        numberOfEdges++;
        String objectAsString = null;
        if (object.isLiteral())
            objectAsString = object.getLiteralValue().toString();
        else
            objectAsString = object.toString();

        if (!replacementsMap.containsKey(subject))
            replacementsMap.put(subject, id++);
        if (!replacementsMap.containsKey(objectAsString))
            replacementsMap.put(objectAsString, id++);

        Integer subjectId = replacementsMap.get(subject);
        Integer objectId = replacementsMap.get(objectAsString);

        LinkedList<Integer> adjacents = null;
        if (!connections.containsKey(subjectId)) {
            adjacents = new LinkedList<>();
            adjacents.add(objectId);
            connections.put(subjectId, adjacents);
        } else {
            adjacents = connections.get(subjectId);
            adjacents.add(objectId);
            connections.replace(subjectId, adjacents);
        }
    }


}
