package tributary;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ConsumerGroup<T> {
    private String id;
    private Topic<T> topic;
    private List<Consumer<T>> consumers;
    private RebalancingMethod rebalancingMethod;

    public ConsumerGroup(String id, Topic<T> topic, RebalancingMethod rebalancingMethod) {
        this.id = id;
        this.topic = topic;
        this.rebalancingMethod = rebalancingMethod;
        this.consumers = new ArrayList<>();
    }

    // method to perfrom rebalancing:
    public void performRebalancing() {
        Map<String, Partition<T>> partitionsMap = topic.getPartitions();
        List<Partition<T>> partitionsAll = new ArrayList<>(partitionsMap.values());
        int count = consumers.size();
        for (int i = 0; i < partitionsAll.size(); i++) {
            Consumer<T> assignedConsumer = consumers.get(i % count);
            Partition<T> partition = partitionsAll.get(i);
            assignedConsumer.addPartition(partition);
        }
        System.out.println("Completed Rebalancing for Consumer Group: " + id);
    }

    public void addConsumer(Consumer<T> consumer) {
        this.consumers.add(consumer);
    }

    public void removeConsumer(Consumer<T> consumer) {
        this.consumers.remove(consumer);
    }

    public List<Consumer<T>> getConsumers() {
        return consumers;
    }

    public void setConsumers(List<Consumer<T>> consumers) {
        this.consumers = consumers;
    }

    public RebalancingMethod getRebalancedMethod() {
        return rebalancingMethod;
    }

    public void setRebalancedMethod(RebalancingMethod rebalancingMethod) {
        this.rebalancingMethod = rebalancingMethod;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Topic<T> getTopic() {
        return topic;
    }

    public void setTopic(Topic<T> topic) {
        this.topic = topic;
    }

}
