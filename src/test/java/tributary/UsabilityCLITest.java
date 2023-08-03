package tributary;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import cli.CLI;

public class UsabilityCLITest {

    @Test
    @Tag("01-1")
    @DisplayName("Testing CLI Usability Example")
    public void testCLIExample() throws Exception {

        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();
        String input = String.join(System.lineSeparator(),
                "createTopic Topic1 String",
                "createPartition Topic1 Partition1",
                "createConsumerGroup Group1 Topic1 ROUNDROBIN",
                "createProducer Producer1 String MANUAL",
                "produceEvent Producer1 Topic1", filePathEvent1, "Partition1",
                "exit"
        );
        ByteArrayInputStream in = new ByteArrayInputStream(input.getBytes());
        System.setIn(in);

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        System.setOut(new PrintStream(out));

        Thread thread = new Thread(() -> CLI.main(new String[]{}));
        thread.start();
        thread.join();

        String output = out.toString();
        assertTrue(output.contains("Exiting Tributary..."));
    }
}
