package base.converters;

import base.Application;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(classes = Application.class)
class RdfToMotivoConverterTest {
    @Resource
    private RdfToMotivoConverter instance;

    @Test
    void convert() {
        instance.convert();
    }
}