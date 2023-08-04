package tributary;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
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

        String input = String.join(System.lineSeparator(),
                "createTopic Topic1 String",
                "createPartition Topic1 Partition1",
                "createConsumerGroup Group1 Topic1 ROUND_Robin",
                "createProducer Producer1 String MANUAL",
                "produceEvent Producer1 Topic1 src/test/resources/event1.json partition1",
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

    @Test
    @Tag("01-2")
    @DisplayName("Testing CLI Consumer Creation")
    public void testCreateConsumer() throws Exception {

        String input = String.join(System.lineSeparator(),
                "createTopic Topic2 String",
                "createConsumerGroup Group2 Topic2 ROUND_ROBIN",
                "createConsumer Consumer1 Group2",
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

    @Test
    @Tag("01-3")
    @DisplayName("Testing CLI Consume Events")
    public void testConsumeEvents() throws Exception {

        String input = String.join(System.lineSeparator(),
                "createTopic Topic3 String",
                "createConsumerGroup Group3 Topic3 ROUND_ROBIN",
                "createConsumer Consumer1 Group3",
                "createPartition Topic3 Partition3",
                "createProducer Producer1 String MANUAL",
                "produceEvent Producer1 Topic3 src/test/resources/event1.json Partition3",
                "consumeEvents Consumer1 Group3 10",
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

    @Test
    @Tag("01-4")
    @DisplayName("Testing CLI Parallel Produce and Consume")
    public void testParallelProduceConsume() throws Exception {

        String input = String.join(System.lineSeparator(),
                "createTopic Topic4 String",
                "createConsumerGroup Group4 Topic4 ROUND_ROBIN",
                "createConsumer Consumer1 Group4",
                "createPartition Topic4 Partition4",
                "createProducer Producer1 String MANUAL",
                "parallelProduce Producer1 Topic4 src/test/resources/event1.json,src/test/resources/event2.json",
                "parallelConsume Consumer1 Group4 20",
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
