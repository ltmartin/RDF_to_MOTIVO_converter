package base;

import base.converters.RdfToMotivoConverter;
import base.utils.QueryExecutor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.jdbc.support.DatabaseStartupValidator;

import javax.annotation.Resource;
import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.*;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.Executor;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * @author Leandro Tabares Martín
 *
 **/
@SpringBootApplication
public class Application implements CommandLineRunner {

    private final Logger logger = Logger.getLogger(Application.class.getName());
    @Resource
    private RdfToMotivoConverter converter;

    @Value("${sparql.endpoint}")
    private String endpoint;
    @Value("${sparql.dataset_IRI}")
    private String datasetIri;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    public void run(String... args) {
        System.out.println("Application started.");
        logger.log(Level.ALL, "Application started.");

        for (String arg : args) {
            if (arg.contains("endpoint")){
                endpoint = arg.substring(arg.indexOf("=")+1);
            }

            if (arg.contains("dataset")){
                datasetIri = arg.substring(arg.indexOf("=")+1);
            }
        }
        logger.log(Level.INFO, "Reading from endpoint: " + endpoint + " and dataset: " + datasetIri);
        converter.convert(endpoint, datasetIri);
    }

}
