package tributary;


import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class ConsumerGroupRebalancingTest {

    @Test
    @Tag("01-01")
    @DisplayName("Testing setConsumerGroupRebalancing method")
    public void testSetConsumerGroupRebalancing() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);

        assertEquals(RebalancingMethod.ROUND_ROBIN, tributary.getConsumerGroups().get("group1").getRebalancedMethod());

        tributary.setConsumerGroupRebalancing("group1", RebalancingMethod.LEAST_LOAD);
        assertEquals(RebalancingMethod.LEAST_LOAD, tributary.getConsumerGroups().get("group1").getRebalancedMethod());

        // Testing with non-existing consumer group
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            tributary.setConsumerGroupRebalancing("nonExistentGroup", RebalancingMethod.LEAST_LOAD);
        });

        String expectedMessage = "Consumer Group: nonExistentGroup cannot be found";
        String actualMessage = exception.getMessage();
        assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test
    @DisplayName("Testing setConsumerGroupRebalancing method with null groupId")
    public void testSetConsumerGroupRebalancingWithNullGroupId() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);

        assertThrows(IllegalArgumentException.class, () -> {
            tributary.setConsumerGroupRebalancing(null, RebalancingMethod.LEAST_LOAD);
        });
    }

    @Test
    @DisplayName("Testing setConsumerGroupRebalancing method with null rebalancing method")
    public void testSetConsumerGroupRebalancingWithNullRebalancingMethod() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);

        assertThrows(IllegalArgumentException.class, () -> {
            tributary.setConsumerGroupRebalancing("group1", null);
        });
    }

    @Test
    @DisplayName("Testing setConsumerGroupRebalancing method with multiple rebalancing methods for same group")
    public void testSetConsumerGroupRebalancingWithMultipleRebalancingMethods() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);

        tributary.setConsumerGroupRebalancing("group1", RebalancingMethod.LEAST_LOAD);
        assertEquals(RebalancingMethod.LEAST_LOAD, tributary.getConsumerGroups().get("group1").getRebalancedMethod());

        tributary.setConsumerGroupRebalancing("group1", RebalancingMethod.ROUND_ROBIN);
        assertEquals(RebalancingMethod.ROUND_ROBIN, tributary.getConsumerGroups().get("group1").getRebalancedMethod());
    }

    @Test
    @DisplayName("Testing setConsumerGroupRebalancing method with multiple consumer groups")
    public void testSetConsumerGroupRebalancingWithMultipleConsumerGroups() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);
        tributary.createConsumerGroup("group2", "topic1", RebalancingMethod.LEAST_LOAD);

        tributary.setConsumerGroupRebalancing("group1", RebalancingMethod.LEAST_LOAD);
        assertEquals(RebalancingMethod.LEAST_LOAD, tributary.getConsumerGroups().get("group1").getRebalancedMethod());

        tributary.setConsumerGroupRebalancing("group2", RebalancingMethod.ROUND_ROBIN);
        assertEquals(RebalancingMethod.ROUND_ROBIN, tributary.getConsumerGroups().get("group2").getRebalancedMethod());
    }
}
