package tributary;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;
import java.util.Map;
import java.util.UUID;

public class Partition<T> {
    private String Id;
    private Queue<Message<T>> messages;
    private Map<String, T> events;
    private Producer<T> producer;

    public Partition(String id) {
        this.Id = id;
        this.messages = new LinkedList<>();
        this.events = new HashMap<>();
        this.producer = producer;
    }

    public void addMessage(Message<T> message) {
        messages.add(message);
    }

    public Message<T> consumeMessage() {
        return messages.poll();
    }

    public String getId() {
        return Id;
    }

    public void setId(String id) {
        this.Id = id;
    }

    public Queue<Message<T>> getMessages() {
        return messages;
    }

    public void setMessages(Queue<Message<T>> messages) {
        this.messages = messages;
    }

    public Map<String, T> getEvents() {
        return events;
    }

    // associating producers and partitions by adding a getProducer method:
    public Producer<T> getProducer() {
        return producer;
    }

    public void setProducer(Producer<T> producer) {
        this.producer = producer;
    }

    // method for adding event:
    public String addEvent(T event) {
        String eventId = UUID.randomUUID().toString();
        this.events.put(eventId, event);
        return eventId;
    }
}