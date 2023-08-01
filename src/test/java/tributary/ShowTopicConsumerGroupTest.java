package tributary;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class ShowTopicConsumerGroupTest {

    @Test
    @Tag("01-1")
    @DisplayName("Testing showTopic method")
    public void testShowTopic() {
        Tributary<String> tributary = new Tributary<>();

        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");

        assertDoesNotThrow(() -> tributary.showTopic("topic1"));
        assertDoesNotThrow(() -> tributary.showTopic("random useless topic Does Not Exist"));
    }

    @Test
    @Tag("01-2")
    @DisplayName("Testing showTopic with a topic that has partitions and events")
    public void testShowTopicWithPartitionsAndEvents() {

        Tributary<String> tributary = new Tributary<>();

        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");
        tributary.createProducer("producer1", "String", "Random");

        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();
        tributary.produceEvent("producer1", "topic1", filePathEvent1, "partition1");

        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));
        tributary.showTopic("topic1");

        String expectedOutput = "Topic: topic1\nPartition: partition1\nEvents: \nEvent ID: "
                            + tributary.getTopics().get("topic1")
                            .getPartition("partition1")
                            .getEvents().keySet().iterator().next()
                            + ", Event: " + tributary.getTopics().get("topic1")
                            .getPartition("partition1")
                            .getEvents().values().iterator().next() + "\n";
        assertEquals(expectedOutput, outContent.toString());
    }

    @Test
    @Tag("01-3")
    @DisplayName("Testing showTopic with non-existent topic")
    public void testShowTopicWithNoTopic() {

        Tributary<String> tributary = new Tributary<>();

        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));

        tributary.showTopic("random topic");

        String expectedOutput = "Topic: random topic not found\n";
        assertEquals(expectedOutput, outContent.toString());
    }

    @Test
    @Tag("01-4")
    @DisplayName("Testing showConsumerGroup method")
    public void testShowConsumerGroup() {
        Tributary<String> tributary = new Tributary<>();

        tributary.createTopic("topic1", "String");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);

        assertDoesNotThrow(() -> tributary.showConsumerGroup("group1"));
        assertDoesNotThrow(() -> tributary.showConsumerGroup("random useless topic Does Not Exist"));
    }

    @Test
    @Tag("01-5")
    @DisplayName("Testing showConsumerGroup with a group that has consumers and partitions")
    public void testShowConsumerGroupWithConsumersAndPartitions() {

        Tributary<String> tributary = new Tributary<>();

        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group1", "consumer1");
        tributary.assignPartitionToConsumer("group1", "consumer1", "partition1");

        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));

        tributary.showConsumerGroup("group1");

        String expectedOutput =
        "Consumer Group: group1\n\tConsumer: consumer1\n\t\tReceiving from Partition: partition1\n";
        assertEquals(expectedOutput, outContent.toString());
    }

    @Test
    @Tag("01-6")
    @DisplayName("Testing showConsumerGroup with non-existent consumer group")
    public void testShowConsumerGroupNoConsumerGroup() {

        Tributary<String> tributary = new Tributary<>();

        ByteArrayOutputStream outContent = new ByteArrayOutputStream();
        System.setOut(new PrintStream(outContent));

        tributary.showConsumerGroup("nonexistentGroup");

        String expectedOutput = "Consumer Group: nonexistentGroup not found\n";
        assertEquals(expectedOutput, outContent.toString());
    }

    @AfterEach
    public void restoreStreams() {
        System.setOut(System.out);
    }
}
