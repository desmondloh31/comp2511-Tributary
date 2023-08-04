package tributary;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;

import java.util.List;

public class Consumer<T> {
    private String id;
    private ConsumerGroup<T> consumerGroup;
    private Map<String, Partition<T>> partitions;
    private List<String> consumedEvents;
    private List<Topic<T>> subscribedTopics;

    public Consumer(String id, ConsumerGroup<T> consumerGroup) {
        this.id = id;
        this.consumerGroup = consumerGroup;
        this.partitions = new HashMap<>();
        this.consumedEvents = new ArrayList<>();
        this.subscribedTopics = new ArrayList<>();
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public ConsumerGroup<T> getConsumerGroup() {
        return consumerGroup;
    }

    public void setConsumerGroup(ConsumerGroup<T> consumerGroup) {
        this.consumerGroup = consumerGroup;
    }

    public Map<String, Partition<T>> getPartitions() {
        return partitions;
    }

    public void addPartition(Partition<T> partition) {
        this.partitions.put(partition.getId(), partition);
    }

    public void addTopic(Topic<T> topic) {
        this.subscribedTopics.add(topic);
    }

    public List<String> getConsumedEvents() {
        return this.consumedEvents;
    }

    // helper method to consume single event:
    public Message<T> consumeEvent(String partitionId) {
        Partition<T> partition = partitions.get(partitionId);
        if (partition != null && !partition.getMessages().isEmpty()) {
            Message<T> message = partition.getMessages().poll();
            consumedEvents.add(message.getValue().toString());
            return message;
        }
        return null;
   }

    // helper method to consume multiple events:
    public List<Message<T>> consumeEvents(String partitionId, int numOfEvents) {
        Partition<T> partition = partitions.get(partitionId);
        List<Message<T>> messagesRemoved = new ArrayList<>();
        if (partition != null) {
            while (!partition.getMessages().isEmpty() && numOfEvents > 0) {
                Message<T> message = partition.getMessages().poll();
                messagesRemoved.add(message);
                consumedEvents.add(message.getValue().toString());
                numOfEvents--;
            }
        }
        return messagesRemoved;
    }
}
