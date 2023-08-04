package tributary;

import java.util.ArrayList;
import java.util.Random;

public class Producer<T> {
    private String id;
    private Class<?> type;
    private AllocationMethod allocationMethod;
    private Tributary<T> tributary;
    private Random random = new Random();

    public Producer(String id, Class<?> type, Tributary<T> tributary, AllocationMethod allocationMethod) {
        this.id = id;
        this.type = type;
        this.allocationMethod = allocationMethod;
        this.tributary = tributary;
    }

    public String produce(Message<T> message) {
        String partitionId = null;
        if (allocationMethod == AllocationMethod.RANDOM) {
            Topic<T> topic = getRandomTopic();
            if (topic != null) {
                Partition<T> partition = getRandomPartition(topic);
                if (partition != null) {
                    partition.addMessage(message);
                    partitionId = partition.getId();
                } else {
                    System.out.println("No partitions in topic " + topic.getId());
                }
            } else {
                System.out.println("No topics Available");
            }
        } else if (allocationMethod == AllocationMethod.MANUAL) {
            var topicsAll = new ArrayList<>(tributary.getTopics().values());
            if (!topicsAll.isEmpty()) {
                var topic = topicsAll.get(0);
                var partitionsAll = new ArrayList<>(topic.getPartitions().values());
                if (!partitionsAll.isEmpty()) {
                    var partition = partitionsAll.get(0);
                    partition.addMessage(message);
                    partitionId = partition.getId();
                } else {
                    System.out.println("No partitions in topic " + topic.getId());
                }
            } else {
                System.out.println("No topics available");
            }
        }
        return partitionId;
    }

    // Helper method to getRandomTopic:
    private Topic<T> getRandomTopic() {
        var topicsAll = new ArrayList<>(tributary.getTopics().values());
        if (topicsAll.isEmpty()) {
            return null;
        }
        return topicsAll.get(random.nextInt(topicsAll.size()));
    }

    // Helper method to getRandomPartition:
    private Partition<T> getRandomPartition(Topic<T> topic) {
        var partitionsAll = new ArrayList<>(topic.getPartitions().values());
        if (partitionsAll.isEmpty()) {
            return null;
        }
        return partitionsAll.get(random.nextInt(partitionsAll.size()));
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Class<?> getType() {
        return type;
    }

    public void setType(Class<?> type) {
        this.type = type;
    }

    public AllocationMethod getAllocationMethod() {
        return allocationMethod;
    }

    public void setAllocationMethod(AllocationMethod allocationMethod) {
        this.allocationMethod = allocationMethod;
    }

}
