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
    public T consumeEvent(String partitionId) {
        Partition<T> partition = partitions.get(partitionId);
        if (partition != null && !partition.getEvents().isEmpty()) {
            Map.Entry<String, T> entry = partition.getEvents().entrySet().iterator().next();
            T event = entry.getValue();
            partition.getEvents().remove(entry.getKey());
            this.consumedEvents.add(event.toString());
            return event;
        }
        return null;
   }

    // helper method to consume multiple events:
    public List<T> consumeEvents(String partitionId, int numOfEvents) {
        Partition<T> partition = partitions.get(partitionId);
        List<T> eventsRemoved = new ArrayList<>();
        if (partition != null) {
            Iterator<Map.Entry<String, T>> eventArrayList = partition.getEvents().entrySet().iterator();
            while (eventArrayList.hasNext() && numOfEvents > 0) {
                Map.Entry<String, T> entry = eventArrayList.next();
                eventsRemoved.add(entry.getValue());
                eventArrayList.remove();
                numOfEvents--;
            }
        }
        return eventsRemoved;
    }
}
