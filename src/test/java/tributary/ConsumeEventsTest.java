package tributary;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class ConsumeEventsTest {
    // need to revise test and method:
    @Test
    @Tag("01-7")
    @DisplayName("Testing consumeEvent method for 3 consumers")
    public void testConsumeEvent() throws IOException {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group1", "consumer1");
        tributary.createProducer("producer1", "String", "Manual");
    
        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();
        tributary.produceEvent("producer1", "topic1", filePathEvent1, "partition1");
    
        tributary.subscribeConsumerToProducer("consumer1", "producer1");

        String consumedEvent = tributary.consumeEvent("consumer1", "partition1");
        assertEquals(filePathEvent1, consumedEvent);
        
    
        // Consuming again should return null, since no more events are left.
        assertNull(tributary.consumeEvent("consumer1", "partition1"));
    }

    // need to revise test and method: 
    @Test
    @Tag("01-8")
    @DisplayName("Testing consumeEvents method for 3 consumers")
    public void testConsumeEvents() throws IOException {
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

        // Note: The events consumed here are not produced in this test, so they are all empty lists.
        assertTrue(tributary.consumeEvents("consumer1", "partition1", 5).isEmpty());
        assertTrue(tributary.consumeEvents("consumer2", "partition2", 5).isEmpty());
        assertTrue(tributary.consumeEvents("consumer3", "partition3", 5).isEmpty());
    }

}
