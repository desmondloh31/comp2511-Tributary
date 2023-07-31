package tributary;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.IntStream;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class ParallelTest {
    
    @Test
    @Tag("01-11")
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

    // need to revise test and method: 
    @Test
    @Tag("01-12")
    @DisplayName("Testing parallelConsume method")
    public void testParallelConsume() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group1", "consumer1");

        tributary.createTopic("topic2", "String");
        tributary.createPartition("topic2", "partition2");
        tributary.createConsumerGroup("group2", "topic2", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group2", "consumer2");

        tributary.createTopic("topic3", "String");
        tributary.createPartition("topic3", "partition3");
        tributary.createConsumerGroup("group3", "topic3", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group3", "consumer3");

        // assume these numbers of events are already in each partition
        List<Integer> numberOfEvents = Arrays.asList(10, 15, 20);

        List<String> consumerIds = Arrays.asList("consumer1", "consumer2", "consumer3");
        List<String> partitionIds = Arrays.asList("partition1", "partition2", "partition3");

        IntStream.range(0, consumerIds.size()).forEach(i -> {
            assertDoesNotThrow(() -> tributary.parallelConsume(consumerIds.get(i), partitionIds.get(i), numberOfEvents.get(i)));
        });
    }
}
