package tributary;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class CreatePartitionTest {

    @Test
    @Tag("01-1")
    @DisplayName("Testing Creating 3 topics and 3 partitions usimg createTopic and createPartition")
    public void testCreatePartition() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");

        tributary.createTopic("topic2", "String");
        tributary.createPartition("topic2", "partition2");

        tributary.createTopic("topic3", "String");
        tributary.createPartition("topic3", "partition3");

        assertTrue(tributary.getTopics().get("topic1").getPartitions().containsKey("partition1"));
        assertTrue(tributary.getTopics().get("topic2").getPartitions().containsKey("partition2"));
        assertTrue(tributary.getTopics().get("topic3").getPartitions().containsKey("partition3"));
    }

    @Test
    @Tag("01-2")
    @DisplayName("Testing createPartition for non-existent topic")
    public void testCreatePartitionNonExistentTopic() {
        Tributary<String> tributary = new Tributary<>();
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            tributary.createPartition("nonExistentTopic", "partition1");
        });

        String expectedMessage = "Topic: nonExistentTopic not found";
        String actualMessage = exception.getMessage();
        assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test
    @Tag("01-3")
    @DisplayName("Testing createPartition with multiple partitions in a single topic")
    public void testCreatePartitionMultiplePartitions() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");
        tributary.createPartition("topic1", "partition2");
        tributary.createPartition("topic1", "partition3");

        assertTrue(tributary.getTopics().get("topic1").getPartitions().containsKey("partition1"));
        assertTrue(tributary.getTopics().get("topic1").getPartitions().containsKey("partition2"));
        assertTrue(tributary.getTopics().get("topic1").getPartitions().containsKey("partition3"));
    }

    @Test
    @Tag("01-4")
    @DisplayName("Testing createPartition with duplicate partition in a topic")
    public void testCreatePartitionDuplicatePartition() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");

        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            tributary.createPartition("topic1", "partition1");
        });

        String expectedMessage = "Partition ID: partition1 already exists in Topic: topic1";
        String actualMessage = exception.getMessage();
        assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test
    @Tag("01-5")
    @DisplayName("Testing createPartition with null partitionId")
    public void testCreatePartitionNullPartitionId() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            tributary.createPartition("topic1", null);
        });

        String expectedMessage = "Partition ID cannot be null";
        String actualMessage = exception.getMessage();
        assertTrue(actualMessage.contains(expectedMessage));
    }
}
