package tributary;

import static org.junit.jupiter.api.Assertions.*;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;


public class ConsumerTest {
    @Test
    @Tag("01-1")
    @DisplayName("Testing creating consumersGroup for 3 groups")
    public void testCreateConsumerGroup() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);

        tributary.createTopic("topic2", "String");
        tributary.createConsumerGroup("group2", "topic2", RebalancingMethod.ROUND_ROBIN);

        tributary.createTopic("topic3", "String");
        tributary.createConsumerGroup("group3", "topic3", RebalancingMethod.ROUND_ROBIN);

        assertNotNull(tributary.getConsumerGroups().get("group1"));
        assertEquals(RebalancingMethod.ROUND_ROBIN, tributary.getConsumerGroups().get("group1").getRebalancedMethod());

        assertNotNull(tributary.getConsumerGroups().get("group2"));
        assertEquals(RebalancingMethod.ROUND_ROBIN, tributary.getConsumerGroups().get("group2").getRebalancedMethod());

        assertNotNull(tributary.getConsumerGroups().get("group3"));
        assertEquals(RebalancingMethod.ROUND_ROBIN, tributary.getConsumerGroups().get("group3").getRebalancedMethod());
    }

    @Test
    @Tag("01-2")
    @DisplayName("Testing CreateConsumer for 3 consumers")
    public void testCreateConsumer() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group1", "consumer1");

        tributary.createTopic("topic2", "String");
        tributary.createConsumerGroup("group2", "topic2", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group2", "consumer2");

        tributary.createTopic("topic3", "String");
        tributary.createConsumerGroup("group3", "topic3", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group3", "consumer3");

        ConsumerGroup<String> consumerGroup1 = tributary.getConsumerGroups().get("group1");
        Consumer<String> consumer1 = findConsumerById(consumerGroup1.getConsumers(), "consumer1");
        assertNotNull(consumer1);

        ConsumerGroup<String> consumerGroup2 = tributary.getConsumerGroups().get("group2");
        Consumer<String> consumer2 = findConsumerById(consumerGroup2.getConsumers(), "consumer2");
        assertNotNull(consumer2);

        ConsumerGroup<String> consumerGroup3 = tributary.getConsumerGroups().get("group3");
        Consumer<String> consumer3 = findConsumerById(consumerGroup3.getConsumers(), "consumer3");
        assertNotNull(consumer3);

    }

    @Test
    @Tag("01-3")
    @DisplayName("Testing deleteConsumer by deleting 3 consumers") 
    public void testDeleteConsumer() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group1", "consumer1");

        tributary.createTopic("topic2", "String");
        tributary.createConsumerGroup("group2", "topic2", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group2", "consumer2");

        tributary.createTopic("topic3", "String");
        tributary.createConsumerGroup("group3", "topic3", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group3", "consumer3");
    
        ConsumerGroup<String> consumerGroup1 = tributary.getConsumerGroups().get("group1");
        assertNotNull(findConsumerById(consumerGroup1.getConsumers(), "consumer1"));
        tributary.deleteConsumer("group1", "consumer1");
        assertNull(findConsumerById(consumerGroup1.getConsumers(), "consumer1"));

        ConsumerGroup<String> consumerGroup2 = tributary.getConsumerGroups().get("group2");
        assertNotNull(findConsumerById(consumerGroup2.getConsumers(), "consumer2"));
        tributary.deleteConsumer("group2", "consumer2");
        assertNull(findConsumerById(consumerGroup1.getConsumers(), "consumer2"));

        ConsumerGroup<String> consumerGroup3 = tributary.getConsumerGroups().get("group3");
        assertNotNull(findConsumerById(consumerGroup3.getConsumers(), "consumer3"));
        tributary.deleteConsumer("group3", "consumer3");
        assertNull(findConsumerById(consumerGroup1.getConsumers(), "consumer3"));
    
    }

    // tests to test non-existent Ids:
    @Test
    @Tag("01-4")
    @DisplayName("Testing createConsumerGroup with non-existent Id")
    public void testCreateConsumerGroupWithNullID() {
        Tributary<String> tributary = new Tributary<>();
        assertThrows(IllegalArgumentException.class, () -> {
            tributary.createConsumerGroup(null, "topic1", RebalancingMethod.ROUND_ROBIN);
        });
    }

    @Test
    @Tag("01-5")
    @DisplayName("Testing createConsumer with null as the ID")
    public void testCreateConsumerWithNullID() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        assertThrows(IllegalArgumentException.class, () -> {
            tributary.createConsumer("group1", null);
        });
    }

    @Test
    @DisplayName("Testing deleteConsumer with null as the ID")
    public void testDeleteConsumerWithNullID() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumer("group1", "consumer1");
        assertThrows(IllegalArgumentException.class, () -> {
            tributary.deleteConsumer("group1", null);
        });
    }

    private Consumer<String> findConsumerById(List<Consumer<String>> consumers, String id) {
        for (Consumer<String> consumer : consumers) {
            if (consumer.getId().equals(id)) {
                return consumer;
            }
        }
        return null;
    }


      
}
