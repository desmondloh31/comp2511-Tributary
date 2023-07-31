package tributary;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class ShowTopicConsumerGroupTest {
    @Test
    @Tag("01-9")
    @DisplayName("Testing showTopic method")
    public void testShowTopic() {
        Tributary<String> tributary = new Tributary<>();

        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");

        assertDoesNotThrow(() -> tributary.showTopic("topic1"));
        assertDoesNotThrow(() -> tributary.showTopic("random useless topic Does Not Exist"));
    }

    @Test
    @Tag("01-10")
    @DisplayName("Testing showConsumerGroup method")
    public void testShowConsumerGroup() {
        Tributary<String> tributary = new Tributary<>();

        tributary.createTopic("topic1", "String");
        tributary.createConsumerGroup("group1", "topic1", RebalancingMethod.ROUND_ROBIN);

        assertDoesNotThrow(() -> tributary.showConsumerGroup("group1"));
        assertDoesNotThrow(() -> tributary.showConsumerGroup("random useless topic Does Not Exist"));
    }
}
