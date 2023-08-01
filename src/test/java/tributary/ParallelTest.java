package tributary;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class ParallelTest {
    
    @Test
    @Tag("01-1")
    @DisplayName("Testing parallelProduce method")
    public void testParallelProduce() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createTopic("topic2", "String");
        tributary.createTopic("topic3", "String");

        tributary.createPartition("topic1", "partition1");
        tributary.createPartition("topic2", "partition2");
        tributary.createPartition("topic3", "partition3");

        tributary.createProducer("producer1", "String", "Random");
        tributary.createProducer("producer2", "String", "Random");
        tributary.createProducer("producer3", "String", "Random");


        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();
        tributary.produceEvent("producer1", "topic1", filePathEvent1, "partition1");

        String basePathEvent2 = new File("").getAbsolutePath();
        String filePathEvent2 = new File(basePathEvent2, "src/test/resources/event2.json").getAbsolutePath();
        tributary.produceEvent("producer2", "topic2", filePathEvent2, "partition2");

        String basePathEvent3 = new File("").getAbsolutePath();
        String filePathEvent3 = new File(basePathEvent3, "src/test/resources/event3.json").getAbsolutePath();
        tributary.produceEvent("producer3", "topic3", filePathEvent3, "partition3");

        List<String> producerIds = Arrays.asList("producer1", "producer2", "producer3");
        List<String> filePaths = Arrays.asList(filePathEvent1, filePathEvent2, filePathEvent3);

        producerIds.forEach(producerId -> {
            assertDoesNotThrow(() -> tributary.parallelProduce(producerId, "topic1", filePaths));
            assertDoesNotThrow(() -> tributary.parallelProduce(producerId, "topic2", filePaths));
            assertDoesNotThrow(() -> tributary.parallelProduce(producerId, "topic3", filePaths));
        });

        assertFalse(tributary.getTopics().get("topic1").getPartition("partition1").getEvents().isEmpty());
        assertFalse(tributary.getTopics().get("topic2").getPartition("partition2").getEvents().isEmpty());
        assertFalse(tributary.getTopics().get("topic3").getPartition("partition3").getEvents().isEmpty());
    }

    @Test
    @Tag("01-2")
    @DisplayName("Testing parallelProducer with non-existing producer and non-existing topic")
    public void testParallelProduceNoProducerNoTopic() {

        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createTopic("topic2", "String");
        tributary.createTopic("topic3", "String");

        tributary.createPartition("topic1", "partition1");
        tributary.createPartition("topic2", "partition2");
        tributary.createPartition("topic3", "partition3");

        tributary.createProducer("producer1", "String", "Random");
        tributary.createProducer("producer2", "String", "Random");
        tributary.createProducer("producer3", "String", "Random");


        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();
        tributary.produceEvent("producer1", "topic1", filePathEvent1, "partition1");

        String basePathEvent2 = new File("").getAbsolutePath();
        String filePathEvent2 = new File(basePathEvent2, "src/test/resources/event2.json").getAbsolutePath();
        tributary.produceEvent("producer2", "topic2", filePathEvent2, "partition2");

        String basePathEvent3 = new File("").getAbsolutePath();
        String filePathEvent3 = new File(basePathEvent3, "src/test/resources/event3.json").getAbsolutePath();
        tributary.produceEvent("producer3", "topic3", filePathEvent3, "partition3");

        List<String> filePaths = Arrays.asList(filePathEvent1, filePathEvent2, filePathEvent3);

        assertThrows(IllegalArgumentException.class, () -> tributary.parallelProduce("non_existing_producer", "topic1", filePaths));
        assertThrows(IllegalArgumentException.class, () -> tributary.parallelProduce("random", "topi2", filePaths));
        assertThrows(IllegalArgumentException.class, () -> tributary.parallelProduce("bad", "topic1", filePaths));

        assertThrows(IllegalArgumentException.class, () -> tributary.parallelProduce("producer1", "non_existing_topic", filePaths));
        assertThrows(IllegalArgumentException.class, () -> tributary.parallelProduce("producer1", "random", filePaths));
        assertThrows(IllegalArgumentException.class, () -> tributary.parallelProduce("producer1", "bad", filePaths));
    }

    @Test
    @Tag("01-3")
    @DisplayName("Testing parallelProducer with invalid events")
    public void testParallelProduceInvalidEvents() {

        Tributary<String> tributary = new Tributary<>();

        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");
        tributary.createProducer("producer1", "String", "Random");

        String basePathInvalidEvent = new File("").getAbsolutePath();
        String filePathInvalidEvent = new File(basePathInvalidEvent, "src/test/resources/invalid_event.json").getAbsolutePath();
        List<String> invalidFilePaths = Arrays.asList(filePathInvalidEvent);

        assertDoesNotThrow(() -> tributary.parallelProduce("producer1", "topic1", invalidFilePaths));
        assertTrue(tributary.getTopics().get("topic1").getPartition("partition1").getEvents().isEmpty());
    }

    @Test
    @Tag("01-4")
    @DisplayName("Testing parallelProducer with empty event list")
    public void testParallelProduceEmptyEventList() {

        Tributary<String> tributary = new Tributary<>();

        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");
        tributary.createProducer("producer1", "String", "Random");

        assertDoesNotThrow(() -> tributary.parallelProduce("producer1", "topic1", new ArrayList<>()));
        assertTrue(tributary.getTopics().get("topic1").getPartition("partition1").getEvents().isEmpty());

    }


    @Test
    @Tag("01-5")
    @DisplayName("Testing parallelConsume method")
    public void testParallelConsume() {

        Tributary<String> tributary = new Tributary<>();
  
        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();
        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");
        tributary.createProducer("producer1", "String", "Manual");
        
        // Produce multiple events:
        int numberOfEvents = 10;
        for (int i = 0; i < numberOfEvents; i++) {
            tributary.produceEvent("producer1", "topic1", filePathEvent1, "partition1");
        }

        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group1", "consumer1");
        tributary.assignPartitionToConsumer("group1", "consumer1", "partition1");

        tributary.parallelConsume("consumer1", "partition1", numberOfEvents);

        assertEquals(numberOfEvents, tributary.getConsumers().get("consumer1").getConsumedEvents().size());

        assertThrows(IllegalArgumentException.class, () -> tributary.parallelConsume("noConsumer", "partition1", numberOfEvents));
        assertThrows(IllegalArgumentException.class, () -> tributary.parallelConsume("consumer1", "noPartition", numberOfEvents));

    }

    @Test
    @Tag("01-6")
    @DisplayName("Testing parallelConsume for more events than available")
    public void testParallelConsumeMoreEvents() {

        Tributary<String> tributary = new Tributary<>();

        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");
        tributary.createProducer("producer1", "String", "Random");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group1", "consumer1");
        tributary.assignPartitionToConsumer("group1", "consumer1", "partition1");

        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();
        tributary.produceEvent("producer1", "topic1", filePathEvent1, "partition1");

        tributary.parallelConsume("consumer1", "partition1", 10);
        
        assertEquals(1, tributary.getConsumers().get("consumer1").getConsumedEvents().size());
    }

    @Test
    @Tag("01-7")
    @DisplayName("Testing parallelConsume with no events")
    public void testParallelConsumeNoEvents() {

        Tributary<String> tributary = new Tributary<>();

        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");
        tributary.createProducer("producer1", "String", "Random");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group1", "consumer1");
        tributary.assignPartitionToConsumer("group1", "consumer1", "partition1");

        tributary.parallelConsume("consumer1", "partition1", 10);
        
        assertEquals(0, tributary.getConsumers().get("consumer1").getConsumedEvents().size());

    }

    @Test
    @Tag("01-8")
    @DisplayName("Testing parallelConsume with events greater than available processors")
    public void testParallelConsumeMoreProcessors() {

        Tributary<String> tributary = new Tributary<>();

        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");
        tributary.createProducer("producer1", "String", "Random");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group1", "consumer1");
        tributary.assignPartitionToConsumer("group1", "consumer1", "partition1");

        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();

        // Producing more events than the number of available processors:
        int numberOfEvents = Runtime.getRuntime().availableProcessors() + 5;
        for (int i = 0; i < numberOfEvents; i++) {
            tributary.produceEvent("producer1", "topic1", filePathEvent1, "partition1");
        }

        tributary.parallelConsume("consumer1", "partition1", numberOfEvents);
        
        assertEquals(numberOfEvents, tributary.getConsumers().get("consumer1").getConsumedEvents().size());
    }
}
