package tributary;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class PlayBackTest {

    @Test
    @Tag("01-1")
    @DisplayName("Playback method tests")
    public void testPlayback() {
        Tributary<String> tributary = new Tributary<>();

        tributary.createTopic("topic1", "String");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group1", "consumer1");
        tributary.createPartition("topic1", "partition1");
        tributary.assignPartitionToConsumer("group1", "consumer1", "partition1");

        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();
        tributary.createProducer("producer1", "String", "Manual");
        tributary.produceEvent("producer1", "topic1", filePathEvent1, "partition1");

        assertDoesNotThrow(() -> {
            tributary.playback("consumer1", "partition1", 0);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            tributary.playback("nonexistent", "partition1", 0);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            tributary.playback("consumer1", "nonexistent", 0);
        });

        assertThrows(IllegalArgumentException.class, () -> {
            tributary.playback("consumer1", "partition1", 100);
        });
    }

    @Test
    @Tag("01-2")
    @DisplayName("testing playback with an offset matching exact event size")
    public void testPlaybackExactOffset() {
        Tributary<String> tributary = new Tributary<>();

        tributary.createTopic("topic1", "String");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group1", "consumer1");
        tributary.createPartition("topic1", "partition1");
        tributary.assignPartitionToConsumer("group1", "consumer1", "partition1");

        Topic<String> topic1 = tributary.getTopic("topic1");
        int offSet = topic1.getPartition("partition1").getEvents().size();
        assertThrows(IllegalArgumentException.class, () -> {
            tributary.playback("consumer1", "partition1", offSet);
        });
    }

    @Test
    @Tag("01-3")
    @DisplayName("testing playback with multiple partitions and consumers")
    public void testPlaybackMultiplePartitionsConsumers() {
        Tributary<String> tributary = new Tributary<>();

        tributary.createTopic("topic1", "String");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group1", "consumer1");
        tributary.createPartition("topic1", "partition1");
        tributary.assignPartitionToConsumer("group1", "consumer1", "partition1");

        tributary.createTopic("topic2", "String");
        tributary.createConsumerGroup("group2", "topic2", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group2", "consumer2");
        tributary.createPartition("topic2", "partition2");
        tributary.assignPartitionToConsumer("group2", "consumer2", "partition2");

        tributary.createProducer("producer1", "String", "Manual");
        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();
        tributary.produceEvent("producer1", "topic1", filePathEvent1, "partition1");

        tributary.createProducer("producer2", "String", "Manual");
        String basePathEvent2 = new File("").getAbsolutePath();
        String filePathEvent2 = new File(basePathEvent2, "src/test/resources/event2.json").getAbsolutePath();
        tributary.produceEvent("producer2", "topic2", filePathEvent2, "partition2");

        assertDoesNotThrow(() -> {
            tributary.playback("consumer1", "partition1", 0);
            tributary.playback("consumer2", "partition2", 0);
        });
    }

    @Test
    @Tag("01-4")
    @DisplayName("testing playback without assigning partitions")
    public void testPlaybackWithoutAssigningPartition() {
        Tributary<String> tributary = new Tributary<>();

        tributary.createTopic("topic1", "String");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group1", "consumer1");
        tributary.createPartition("topic1", "partition1");

        assertThrows(IllegalArgumentException.class, () -> {
            tributary.playback("consumer1", "partition1", 0);
        });
    }

}
