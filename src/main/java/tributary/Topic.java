package tributary;

import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.ArrayList;
import java.util.Collections;

public class Topic<T> {
    private String Id;
    private Class<?> type;
    private Map<String, Partition<T>> partitions;
    private Random random = new Random();

    public Topic(String id, Class<?> type) {
        this.Id = id;
        this.type = type;
        this.partitions = new HashMap<>();
    }

    public String getId() {
        return Id;
    }

    public void setId(String id) {
        this.Id = id;
    }

    public Class<?> getType() {
        return type;
    }

    public void setType(Class<T> type) {
        this.type = type;
    }

    public Map<String, Partition<T>> getPartitions() {
        return Collections.unmodifiableMap(partitions);
    }

    public void addPartition(String partitionId) {
        if (!partitions.containsKey(partitionId)) {
            partitions.put(partitionId, new Partition<T>(partitionId));
        }
    }

    public Partition<T> getRandomPartition() {
        if(partitions.isEmpty()){
            return null;
        }
        ArrayList<String> keys = new ArrayList<>(partitions.keySet());
        String randomKey = keys.get(random.nextInt(keys.size()));
        return partitions.get(randomKey);
    }


    public Partition<T> getPartition(String partitionId) {
        return partitions.get(partitionId);
    }

    public void removePartition(String partitionId) {
        this.partitions.remove(partitionId);
    }

    public void setPartitions(Map<String, Partition<T>> partitions) {
        this.partitions = partitions;
    }

}