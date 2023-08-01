package tributary;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.File;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class ConsumeEventsTest {

    @Test
    @Tag("01-1")
    @DisplayName("Testing consumeEvent method for 3 consumers")
    public void testConsumeEvent() {

        Tributary<String> tributary = new Tributary<>();
        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();
        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");
        tributary.createProducer("producer1", "String", "Manual");
        tributary.produceEvent("producer1", "topic1", filePathEvent1, "partition1");

        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group1", "consumer1");
        tributary.createConsumer("group1", "consumer2");
        tributary.createConsumer("group1", "consumer3");

        tributary.assignPartitionToConsumer("group1", "consumer1", "partition1");
        tributary.assignPartitionToConsumer("group1", "consumer2", "partition1");
        tributary.assignPartitionToConsumer("group1", "consumer3", "partition1");

        tributary.consumeEvent("consumer1", "partition1");
        assertEquals(1, tributary.getConsumers().get("consumer1").getConsumedEvents().size());
        assertThrows(IllegalArgumentException.class, () -> tributary.consumeEvent("noConsumer", "partition1"));
    }

    @Test
    @Tag("01-2")
    @DisplayName("Testing consumeEvent for unassigned partition")
    public void testConsumeEventUnassignedPartition() {

        Tributary<String> tributary = new Tributary<>();
        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();
        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");
        tributary.createProducer("producer1", "String", "Manual");
        tributary.produceEvent("producer1", "topic1", filePathEvent1, "partition1");

        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group1", "consumer1");

        assertThrows(IllegalArgumentException.class, () -> tributary.consumeEvent("consumer1", "partition1"));
    }

    @Test
    @Tag("01-3")
    @DisplayName("Testing consumeEvent for a consumer consuming a non-existent partition")
    public void testConsumeEventNoPartition() {

        Tributary<String> tributary = new Tributary<>();
        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();
        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");
        tributary.createProducer("producer1", "String", "Manual");
        tributary.produceEvent("producer1", "topic1", filePathEvent1, "partition1");

        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group1", "consumer1");

        assertThrows(IllegalArgumentException.class, () -> tributary.consumeEvent("consumer1", "no partition"));
    }

    @Test
    @Tag("01-4")
    @DisplayName("Testing consumeEvents method for 3 consumers")
    public void testConsumeEvents() {

        Tributary<String> tributary = new Tributary<>();
        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();
        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");
        tributary.createProducer("producer1", "String", "Manual");
        tributary.produceEvent("producer1", "topic1", filePathEvent1, "partition1");

        for (int i = 0; i < 3; i++) {
            tributary.produceEvent("producer1", "topic1", filePathEvent1, "partition1");
        }

        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group1", "consumer1");
        tributary.assignPartitionToConsumer("group1", "consumer1", "partition1");

        tributary.consumeEvents("consumer1", "partition1", 3);
        assertEquals(3, tributary.getConsumers().get("consumer1").getConsumedEvents().size());
        assertThrows(IllegalArgumentException.class, () -> tributary.consumeEvents("noConsumer", "partition1", 1));
    }


    @Test
    @Tag("01-5")
    @DisplayName("Testing consumeEvents for a consumer trying to consume more events than available in partition")
    public void testConsumeEventsMoreThanAvailable() {

        Tributary<String> tributary = new Tributary<>();
        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();
        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");
        tributary.createProducer("producer1", "String", "Manual");
        tributary.produceEvent("producer1", "topic1", filePathEvent1, "partition1");

        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group1", "consumer1");
        tributary.assignPartitionToConsumer("group1", "consumer1", "partition1");

        tributary.consumeEvents("consumer1", "partition1", 3);
        assertEquals(1, tributary.getConsumers().get("consumer1").getConsumedEvents().size());

    }

}
