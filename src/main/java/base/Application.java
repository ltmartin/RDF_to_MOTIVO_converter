package base;

import base.converters.RdfToMotivoConverter;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.Resource;

/**
 * @author Leandro Tabares Martín
 *
 **/
@SpringBootApplication
public class Application implements CommandLineRunner {

    @Resource
    private RdfToMotivoConverter converter;

    public static void main(String[] args) {
        SpringApplication.run(Application.class, args);
    }

    public void run(String... args) {
        converter.convert();
    }
}
