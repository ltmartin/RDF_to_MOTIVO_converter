package base.converters;

import base.Application;
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
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.logging.Level;
import java.util.logging.Logger;

@Component
@Lazy
public class RdfToMotivoConverter {
    private final Logger logger = Logger.getLogger(Application.class.getName());

    private final static Charset ENCODING = StandardCharsets.UTF_8;
    @Value("${sparql.endpoint}")
    private String endpoint;
    @Value("${sparql.dataset_IRI}")
    private String datasetIri;

    private ConcurrentMap<String, Integer> replacementsMap;
    private ConcurrentMap<Integer, Set<Integer>> connections;
    private Integer id = 0;
    private Integer numberOfEdges = 0;
    private Set<Triple> triples;

    public RdfToMotivoConverter() {
        replacementsMap = new ConcurrentHashMap();
        connections = new ConcurrentHashMap<>();
    }

    public void convert() {
        System.out.println("Convert");
        logger.log(Level.ALL, "Convert");
        final String triplesCountQuery = "SELECT (count(*) AS " + QueryExecutor.COUNT_VAR_NAME + ") FROM <" + datasetIri + "> WHERE {?s ?p ?o}";
        final QueryExecutor countQueryExecutor = new QueryExecutor(endpoint, triplesCountQuery, datasetIri, 0);
        final Integer amountOfTriplesInDataset = countQueryExecutor.runCountQuery();

        final QueryExecutor queryExecutor = new QueryExecutor(endpoint, "",datasetIri, amountOfTriplesInDataset);
        triples = new ForkJoinPool().invoke(queryExecutor);

        triples.stream().forEach(triple -> {
            createReplacement(triple);
        });

        writeInputFile();
        writeReplacementsFile();
    }

    private void writeReplacementsFile() {
        final String fileName = "output/replacements.txt";
        File file = new File(fileName);
        try {
            if (file.createNewFile()){
                BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, ENCODING));
                replacementsMap.forEach((original, replacement) ->{
                    try {
                        writer.write(original + " " + Integer.toString(replacement));
                        writer.newLine();
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                });
                writer.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private void writeInputFile() {
        final String fileName = "output/input.txt";
        File file = new File(fileName);
        try {
            if (file.createNewFile()){
                BufferedWriter writer = new BufferedWriter(new FileWriter(fileName, ENCODING));
                writer.write(id+1 + " " + numberOfEdges);
                writer.newLine();
                for (int i = 0; i < id; i++) {
                    Set<Integer> adjacents = connections.get(i);
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

        Set<Integer> adjacents = null;
        if (!connections.containsKey(subjectId)) {
            adjacents = new HashSet<>();
            adjacents.add(objectId);
            connections.put(subjectId, adjacents);
        } else {
            adjacents = connections.get(subjectId);
            adjacents.add(objectId);
            connections.replace(subjectId, adjacents);
        }
    }


}
