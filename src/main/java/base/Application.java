package base;

import base.converters.RdfToMotivoConverter;
import base.utils.QueryExecutor;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.Resource;
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

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    public void run(String... args) {
        System.out.println("Application started.");
        logger.log(Level.ALL, "Application started.");
        converter.convert();
    }
}
