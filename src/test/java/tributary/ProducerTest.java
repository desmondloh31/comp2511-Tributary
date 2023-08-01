package tributary;

import static org.junit.jupiter.api.Assertions.*;

import java.io.File;
import java.io.IOException;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class ProducerTest {

    @Test
    @Tag("01-1")
    @DisplayName("Testing createProducer")
    public void testCreateProducer() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createProducer("producer1", "String", "Manual");

        Producer<String> producer = tributary.getProducers().get("producer1");
        assertNotNull(producer);
        assertEquals("producer1", producer.getId());
        assertEquals(String.class, producer.getType());
        assertEquals(AllocationMethod.MANUAL, producer.getAllocationMethod());

        tributary.createProducer("producer2", "String", "Random");
        Producer<String> producer2 = tributary.getProducers().get("producer2");
        assertNotNull(producer2);
        assertEquals("producer2", producer2.getId());
        assertEquals(String.class, producer2.getType());
        assertEquals(AllocationMethod.RANDOM, producer2.getAllocationMethod());

        // creating a producer with an invalid type and allocation:
        tributary.createProducer("producer3", "randomizing", "random");
        assertNull(tributary.getProducers().get("producer3"));
    }

    @Test
    @Tag("01-2")
    @DisplayName("Testing createProducer with invalid type")
    public void testCreateProducerInvalidType() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createProducer("producer1", "random type", "Manual");

        assertNull(tributary.getProducers().get("producer1"));
    }

    @Test
    @Tag("01-3")
    @DisplayName("Testing createProducer with invalid allocation")
    public void testCreateProducerInvalidAllocation() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createProducer("producer2", "String", "wrong allocation");

        assertNull(tributary.getProducers().get("producer2"));
    }

    @Test
    @Tag("01-4")
    @DisplayName("Testing produceEvent method for 3 producers")
    public void testProduceEvent() throws IOException {
        Tributary<String> tributary = new Tributary<>();

        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();
        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");
        tributary.createProducer("producer1", "String", "Manual");
        tributary.produceEvent("producer1", "topic1", filePathEvent1, "partition1");


        String basePathEvent3 = new File("").getAbsolutePath();
        String filePathEvent3 = new File(basePathEvent3, "src/test/resources/event3.json").getAbsolutePath();
        tributary.createTopic("topic3", "String");
        tributary.createPartition("topic3", "partition3");
        tributary.createProducer("producer3", "String", "Manual");
        tributary.produceEvent("producer3", "topic3", filePathEvent3, "partition3");

        assertFalse(tributary.getTopics().get("topic1").getPartition("partition1").getEvents().isEmpty());
        assertFalse(tributary.getTopics().get("topic3").getPartition("partition3").getEvents().isEmpty());
    }

    @Test
    @Tag("01-5")
    @DisplayName("Testing produceEvent with non-existent producer")
    public void testProduceEventNoProducer() throws IOException {
        Tributary<String> tributary = new Tributary<>();

        tributary.createTopic("topic1", "String");
        tributary.createTopic("topic2", "String");
        tributary.createTopic("topic3", "String");
        tributary.createPartition("topic1", "partition1");
        tributary.createPartition("topic2", "partition2");
        tributary.createPartition("topic3", "partition3");

        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();
        String basePathEvent2 = new File("").getAbsolutePath();
        String filePathEvent2 = new File(basePathEvent2, "src/test/resources/event2.json").getAbsolutePath();
        String basePathEvent3 = new File("").getAbsolutePath();
        String filePathEvent3 = new File(basePathEvent3, "src/test/resources/event3.json").getAbsolutePath();


        tributary.produceEvent("producer1", "topic1", filePathEvent1, "partition1");
        tributary.produceEvent("producer2", "topic2", filePathEvent2, "partition2");
        tributary.produceEvent("producer3", "topic3", filePathEvent3, "partition3");

        assertTrue(tributary.getTopics().get("topic1").getPartition("partition1").getEvents().isEmpty());
        assertTrue(tributary.getTopics().get("topic2").getPartition("partition2").getEvents().isEmpty());
        assertTrue(tributary.getTopics().get("topic3").getPartition("partition3").getEvents().isEmpty());
    }

    @Test
    @Tag("01-6")
    @DisplayName("Testing produceEvent with non-existent topic")
    public void testProduceEventNoTopic() throws IOException {
        Tributary<String> tributary = new Tributary<>();

        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();

        tributary.createProducer("producer1", "String", "Manual");
        tributary.produceEvent("producer1", "no topic", filePathEvent1, "partition1");

        assertNull(tributary.getTopics().get("no topic"));
    }

    @Test
    @Tag("01-7")
    @DisplayName("Testing produceEvent with non-existent partition")
    public void testProduceEventNonExistentPartition() throws IOException {
        Tributary<String> tributary = new Tributary<>();

        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();

        tributary.createTopic("topic1", "String");
        tributary.createProducer("producer1", "String", "Manual");

        tributary.produceEvent("producer1", "topic1", filePathEvent1, "nonExistentPartition");

        assertNull(tributary.getTopics().get("topic1").getPartition("nonExistentPartition"));
    }

    @Test
    @Tag("01-8")
    @DisplayName("Testing produceEvent with random allocation method")
    public void testProduceEventRandomAllocation() throws IOException {
        Tributary<String> tributary = new Tributary<>();

        String basePathEvent1 = new File("").getAbsolutePath();
        String filePathEvent1 = new File(basePathEvent1, "src/test/resources/event1.json").getAbsolutePath();

        tributary.createTopic("topic1", "String");
        tributary.createPartition("topic1", "partition1");
        tributary.createProducer("producer1", "String", "Random");

        tributary.produceEvent("producer1", "topic1", filePathEvent1, "partition1");

        assertFalse(tributary.getTopics().get("topic1").getPartition("partition1").getEvents().isEmpty());
    }

}
