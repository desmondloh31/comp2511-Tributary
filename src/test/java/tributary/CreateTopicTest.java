package tributary;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

public class CreateTopicTest {

    @Test
    @Tag("01-1")
    @DisplayName("Testing Creating 3 topics using createTopic")
    public void testCreateTopic() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("topic1", "String");
        tributary.createTopic("topic2", "String");
        tributary.createTopic("topic3", "String");

        assertNotNull(tributary.getTopics().get("topic1"));
        assertNotNull(tributary.getTopics().get("topic2"));
        assertNotNull(tributary.getTopics().get("topic3"));

        assertEquals(String.class, tributary.getTopics().get("topic1").getType());
        assertEquals(String.class, tributary.getTopics().get("topic2").getType());
        assertEquals(String.class, tributary.getTopics().get("topic3").getType());
    }

    @Test
    @Tag("01-2")
    @DisplayName("Testing creating an Integer topic using createTopic")
    public void testCreateIntegerTopic() {
        Tributary<Integer> tributary = new Tributary<>();
        tributary.createTopic("topic1Integer", "Integer");
        tributary.createTopic("topic2Integer", "Integer");
        tributary.createTopic("topic3Integer", "Integer");

        assertNotNull(tributary.getTopics().get("topic1Integer"));
        assertEquals(Integer.class, tributary.getTopics().get("topic1Integer").getType());

        assertNotNull(tributary.getTopics().get("topic2Integer"));
        assertEquals(Integer.class, tributary.getTopics().get("topic2Integer").getType());

        assertNotNull(tributary.getTopics().get("topic3Integer"));
        assertEquals(Integer.class, tributary.getTopics().get("topic3Integer").getType());

    }

    @Test
    @Tag("01-3")
    @DisplayName("Testing creating a topic with an invalid type using createTopic")
    public void testCreateInvalidTypeTopic() {
        Tributary<Double> tributary = new Tributary<>();
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            tributary.createTopic("random topic", "Double");
        });

        String expectedMessage = "Type must be either 'Integer' or 'String'";
        String actualMessage = exception.getMessage();
        assertTrue(actualMessage.contains(expectedMessage));
    }

    @Test
    @Tag("01-4")
    @DisplayName("Testing creating an identical topic using createTopic")
    public void testCreateDuplicateTopic() {
        Tributary<String> tributary = new Tributary<>();
        tributary.createTopic("same topic", "String");
        Exception exception = assertThrows(IllegalArgumentException.class, () -> {
            tributary.createTopic("same topic", "String");
        });

        String expectedMessage = "Topic ID: " + "same topic" + " already exists";
        String actualMessage = exception.getMessage();
        assertTrue(actualMessage.contains(expectedMessage));
    }
}
