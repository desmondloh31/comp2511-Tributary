package tributary;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import javax.servlet.http.Part;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.json.JSONObject;
import org.json.JSONTokener;

public class Tributary<T>{
    private Map<String, Topic<T>> topics;
    private Map<String, ConsumerGroup<T>> consumerGroups;
    private Map<String, Producer<T>> producers;
    private Map<String, Consumer<T>> consumers;
    private Map<String, Partition<T>> partitions;

    public Tributary() {
        this.topics = new HashMap<>();
        this.consumerGroups = new HashMap<>();
        this.producers = new HashMap<>();
        this.consumers = new HashMap<>();
    }

    public void createTopic(String id, String type) {
        if (topics.containsKey(id)) {
            throw new IllegalArgumentException("Topic ID: " + id + " already exists");
        }
        Class<?> typeClass;
        if (type.equals("Integer")) {
            typeClass = Integer.class;
        } else if (type.equals("String")) {
            typeClass = String.class;
        } else {
            throw new IllegalArgumentException("Type must be either 'Integer' or 'String'");
        }
        
        Topic<T> topic = new Topic<T>(id, typeClass);
        topics.put(id, topic);
        System.out.println("Topic created with ID: " + id + " and Type: " + type);
    }

    public void createPartition(String topicId, String partitionId) {
        if (topicId == null || topicId.trim().isEmpty()) {
            throw new IllegalArgumentException("Topic ID cannot be null or empty");
        }
        if (partitionId == null || partitionId.trim().isEmpty()) {
            throw new IllegalArgumentException("Partition ID cannot be null or empty");
        }
        
        Topic<T> topic = topics.get(topicId);
        if (topic == null) {
            throw new IllegalArgumentException("Topic: " + topicId + " not found");
        }
        
        if (topic.getPartitions().containsKey(partitionId)) {
            throw new IllegalArgumentException("Partition ID: " + partitionId + " already exists in Topic: " + topicId);
        }
        
        topic.addPartition(partitionId);
        System.out.println("Partition: " + partitionId + " created in Topic: " + topicId);
    }

    public void createConsumerGroup(String id, String topicId, RebalancingMethod rebalancing) {
        Topic<T> topic = topics.get(topicId);
        if (topic == null) {
            throw new IllegalArgumentException("Topic: " + topicId + " not found");
        }
        ConsumerGroup<T> consumerGroup = new ConsumerGroup<T>(id, topic, rebalancing);
        consumerGroups.put(id, consumerGroup);
        System.out.println("Consumer Group created with ID: " + id);
        
    }

    public void createConsumer(String groupId, String consumerId) {
        if (consumerId == null) {
            throw new IllegalArgumentException("Consumer Id cannot be null");
        }
        ConsumerGroup<T> consumerGroup = consumerGroups.get(groupId);
        if (consumerGroup == null) {
            throw new IllegalArgumentException("Consumer Group: " + groupId + " not found");
        }
        Consumer<T> newConsumer = new Consumer<T>(consumerId, consumerGroup);
        consumerGroup.addConsumer(newConsumer);
        consumers.put(consumerId, newConsumer);
        System.out.println("Consumer: " + consumerId + " created in Consumer Group: " + groupId);
        
    }

    public void deleteConsumer(String groupId, String consumerId) {
        ConsumerGroup<T> consumerGroup = consumerGroups.get(groupId);
        if (consumerGroup == null) {
            throw new IllegalArgumentException("Consumer Group: " + groupId + " cannot be found");
        }
        Consumer<T> removingConsumer = null;
        for (Consumer<T> consumer : consumerGroup.getConsumers()) {
            if (consumer.getId().equals(consumerId)) {
                removingConsumer = consumer;
                break;
            }
        }
        if (removingConsumer == null) {
            throw new IllegalArgumentException("Consumer: " + consumerId + " cannot be found in Consumer Group: " + groupId);
        }
        consumerGroup.getConsumers().remove(removingConsumer);
        consumers.remove(consumerId);
        System.out.println("Consumer: " + consumerId + " removed from Consumer Group: " + groupId);
        consumerGroup.performRebalancing();
        System.out.println("Consumer Group: " + groupId + " has been successfully rebalnced");
          
    }

    public void createProducer(String id, String type, String allocation) {
        Class<?> classType; 
        if (type.equals("Integer")) {
            classType = Integer.class;
        } else if (type.equals("String")) {
            classType = String.class;
        } else {
            System.out.println("Type must either be 'Integer' or 'String'");
            return;
        }
        // handling the allocation:
        AllocationMethod allocationMethod;
        if (allocation.toUpperCase().equals("RANDOM")) {
            allocationMethod = AllocationMethod.RANDOM;
        } else if (allocation.toUpperCase().equals("MANUAL")) {
            allocationMethod = AllocationMethod.MANUAL;
        } else {
            System.out.println("Allocation must either be 'Random' or 'Manual'");
            return;
        }
        Producer<T> newProducer = new Producer<>(id, classType, this, allocationMethod);
        producers.put(id, newProducer);
        System.out.println("Producer created with ID: " + id + " and Type: " + type);

    }
    
    public void produceEvent(String producerId, String topicId, String eventFileName, String partitionId) {
        System.out.println("Attempting to produce event with producerId: " + producerId + ", topicId: " + topicId + ", eventFileName: " + eventFileName + ", partitionId: " + partitionId);
        Producer<T> producer = producers.get(producerId);
        Topic<T> topic = topics.get(topicId);
        if (producer == null) {
            System.out.println("Producer: " + producerId + " cannot be found");
            return;
        } 
        if (topic == null) {
            System.out.println("Topic: " + topicId + " cannot be found");
            return;
        }

        @SuppressWarnings("unchecked")
        T event = parseJsonToEvent(eventFileName, (Class<T>)producer.getType());
        if (event == null) {
            System.out.println("Event file: " + eventFileName + " could not be parsed to type: " + producer.getType());
            return;
        }
        System.out.println("Parsed event: " + event);

        Partition<T> partition = topic.getPartition(partitionId);
        if (producer.getAllocationMethod() == AllocationMethod.MANUAL && partition != null) {
            String eventId = partition.addEvent(event);
            System.out.println("Event id: " + eventId + ", was added to a partition: " + partitionId);
        } else if (producer.getAllocationMethod() == AllocationMethod.RANDOM) {
            producer.produce(event);
            System.out.println("Event id: " + event + ", was added to a random partition");
        } else {
            System.out.println("Manual partition: " + partitionId + " cannot be found or allocation method is not manual");
        }

        if (partition != null) {
            System.out.println("Current events in partition: " + partition.getEvents()); 
            System.out.println("Events in partition " + partitionId + ": " + partition.getEvents());
        }
    }

    public T consumeEvent(String consumerId, String partitionId) {
        Consumer<T> consumer = consumers.get(consumerId);
        if (consumer != null) {
            Partition<T> partition = consumer.getPartitions().get(partitionId);
            if (partition != null) {
                T consumedEvent = consumer.consumeEvent(partitionId);
                if (consumedEvent != null) {
                    System.out.println("Consumer: " + consumerId + " consumed event from partition: " + partitionId);
                } else {
                    System.out.println("No event to consume from partition: " + partitionId);
                }
                return consumedEvent;
            } else {
                System.out.println("Partition: " + partitionId + " cannot be found for Consumer: " + consumerId);
            }
        } else {
            System.out.println("Consumer: " + consumerId + " cannot be found");
        }
        return null;
    }

    public List<T> consumeEvents(String consumerId, String partitionId, int numberOfEvents) {
        Consumer<T> consumer = consumers.get(consumerId);
        if (consumer != null) {
            List<T> consumedEvents = consumer.consumeEvents(partitionId, numberOfEvents);
            if (!consumedEvents.isEmpty()) {
                System.out.println("Consumer: " + consumerId + " consumed " + consumedEvents.size() + " events from partition: " + partitionId);
            } else {
                System.out.println("No events to consume from partition: " + partitionId);
            }
            return consumedEvents;
        } else {
            System.out.println("Consumer: " + consumerId + " cannot be found");
        }
        return new ArrayList<>();
    }

    public void showTopic(String topicId) {
        Topic<T> topic = topics.get(topicId);
        if (topic == null) {
            System.out.println("Topic: " + topicId + " not found");
            return;
        }
        System.out.println("Topic: " + topicId);
        for (Map.Entry<String, Partition<T>> entry : topic.getPartitions().entrySet()) {
            System.out.println("Partition: " + entry.getKey());
            Map<String, T> eventsAll = entry.getValue().getEvents();
            if (!eventsAll.isEmpty()) {
                System.out.println("Events: ");
                for (Map.Entry<String, T> eventEntry : eventsAll.entrySet()) {
                    System.out.println("Event ID: " + eventEntry.getKey() + ", Event: " + eventEntry.getValue());
                }
            } else {
                System.out.println("No events in this partition");
            }
        }
    }

    public void showConsumerGroup(String groupId) {
        ConsumerGroup<T> consumerGroup = consumerGroups.get(groupId);
        if (consumerGroup != null) {
            System.out.println("Consumer Group: " + groupId);
            for (Consumer<T> consumer : consumerGroup.getConsumers()) {
                System.out.println("\tConsumer: " + consumer.getId());
                for (Map.Entry<String, Partition<T>> entry : consumer.getPartitions().entrySet()) {
                    System.out.println("\t\tReceiving from Partition: " + entry.getKey());
                }
            }
        } else {
            System.out.println("Consumer Group: " + groupId + " not found");
        }
    }

    // parallel produce (<producer>, <topic>, <event>), ...
    public void parallelProduce(String producerId, String topicId, List<String> eventFileNames) {
        Producer<T> producer = producers.get(producerId);
        Topic<T> topic = topics.get(topicId);
        if (producer == null) {
            System.out.println("Producer: " + producerId + " cannot be found");
            return;
        }
        if (topic == null) {
            System.out.println("Topic: " + topicId + " cannot be found");
            return;
        }
        ExecutorService executorService = Executors.newFixedThreadPool(eventFileNames.size());
        for (String eventFileName : eventFileNames) {
            executorService.submit(() -> {
                @SuppressWarnings("unchecked")
                T event = parseJsonToEvent(eventFileName, (Class<T>)producer.getType());
                if (event == null) {
                    System.out.println("Event file: " + eventFileName + " could not be parsed to type: " + producer.getType());
                    return;
                }
                System.out.println("Parsed event: " + event);
                if (producer.getAllocationMethod() == AllocationMethod.RANDOM) {
                    String partitionId = producer.produce(event);
                    System.out.println("Event id: " + event + ", was added to a random partition: " + partitionId);
                } else {
                    System.out.println("Allocation method was not random"); 
                }
            });
        }
        executorService.shutdown();
    }

    public void parallelConsume(String consumerId, String partitionId, int numberOfEvents) {
        Consumer<T> consumer = consumers.get(consumerId);
        if (consumer == null) {
            System.out.println("Consumer: " + consumerId + " cannot be found");
            return;
        }
        Partition<T> partition = consumer.getPartitions().get(partitionId);
        if (partition == null) {
            System.out.println("Partition: " + partitionId + " cannot be found");
            return;
        }
        ExecutorService executorService = Executors.newFixedThreadPool(numberOfEvents);
        for (int count = 0; count < numberOfEvents; count++) {
            executorService.submit(() -> {
                T event = consumer.consumeEvent(partitionId);
                if (event == null) {
                    System.out.println("No more events to consume from partition: " + partitionId);
                } else {
                    System.out.println("Consumer: " + consumerId + " consumed event from partition: " + partitionId);
                }
            });
        }
        executorService.shutdown();
    }

    public void setConsumerGroupRebalancing(String groupId, RebalancingMethod rebalancingMethod) {
        if (rebalancingMethod == null) {
           throw new IllegalArgumentException("Rebalancing method cannot be null");
        }
        ConsumerGroup<T> consumerGroup = consumerGroups.get(groupId);
        if (consumerGroup == null) {
            throw new IllegalArgumentException("Consumer Group: " + groupId + " cannot be found");
        }
        consumerGroup.setRebalancedMethod(rebalancingMethod);
        System.out.println("Rebalancing method for Consumer Group: " + groupId + " has been set to: " + rebalancingMethod);
    }

    public void playback(String consumerId, String partitionId, int offset) {
        Consumer<T> consumer = consumers.get(consumerId);
        if (consumer == null) {
            throw new IllegalArgumentException("Consumer: " + consumerId + " cannot be found");
        }
        Partition<T> partition = consumer.getPartitions().get(partitionId);
        if (partition == null) {
            throw new IllegalArgumentException("Partition: " + partitionId + " cannot be found for consumer: " + consumerId);
        }
        Map<String, T> events = partition.getEvents();
        if (events.size() <= offset) {
            throw new IllegalArgumentException("Offset is out of range for partition: " + partitionId);
        }
        int index = 0;
        for (Map.Entry<String, T> event : events.entrySet()) {
            if (index >= offset) {
                System.out.println("Event ID: " + event.getKey() + ", Event content: " + event.getValue());
            }
            index++;
        }
    }

    // helper method to parse JSON to event using JSON object:
    public T parseJsonToEvent(String eventFilePath, Class<T> classType) {
        try {
            InputStream inputStream = new FileInputStream(eventFilePath);
            InputStreamReader streamReader = new InputStreamReader(inputStream);
            JSONObject jsonObject = new JSONObject(new JSONTokener(streamReader));
            if (classType == String.class) {
                return classType.cast(jsonObject.getString("value"));
            } else if (classType == Integer.class) {
                return classType.cast(jsonObject.getInt("value"));
            }
        } catch (Exception exception) {
            System.out.println("Error reading or parsing JSON from file: " + eventFilePath);
            exception.printStackTrace();
        }
        return null;
    }

    // helper method to assign a partition to consumer:
    public void assignPartitionToConsumer(String groupId, String consumerId, String partitionId) {
        ConsumerGroup<T> consumerGroup = consumerGroups.get(groupId);
        if (consumerGroup == null) {
            throw new IllegalArgumentException("Consumer Group: " + groupId + " cannot be found");
        }
        Consumer<T> consumer = consumers.get(consumerId);
        if (consumer == null) {
            throw new IllegalArgumentException("Consumer: " + consumerId + " cannot be found");
        }
        Topic<T> topic = consumerGroup.getTopic();
        Partition<T> partition = topic.getPartitions().get(partitionId);
        if (partition == null) {
            throw new IllegalArgumentException("Partition: " + partitionId + " cannot be found in Topic: " + topic.getId());
        }
        consumer.addPartition(partition);
        consumerGroup.performRebalancing();
    }

    // helper method to subscribeConsumerToTopic:
    public void subscribeConsumerToTopic(String consumerId, String topicId) {
        Consumer<T> consumer = consumers.get(consumerId);
        Topic<T> topic = topics.get(topicId);
        if (consumer == null) {
            System.out.println("Consumer: " + consumerId + " not found");
            return;
        }
        if (topic == null) {
            System.out.println("Topic: " + topicId + " not found");
            return;
        }
        consumer.addTopic(topic);
        System.out.println("Consumer: " + consumerId + " subscribed to topic: " + topicId);
    }

    // helper method to subscribeConsumerToProducer:
    public void subscribeConsumerToProducer(String consumerId, String producerId) {
        Consumer<T> consumer = consumers.get(consumerId);
        Producer<T> producer = producers.get(producerId);
        if (consumer == null) {
            System.out.println("Consumer: " + consumerId + " not found");
            return;
        }
        if (producer == null) {
            System.out.println("Producer: " + producerId + " not found");
            return;
        }
        for (Topic<T> topic : topics.values()) {
            for (Partition<T> partition : topic.getPartitions().values()) {
                if (partition.getProducer() != null && partition.getProducer().equals(producer)) {
                    consumer.addTopic(topic);
                    consumer.addPartition(partition);
                    System.out.println("Consumer: " + consumerId + " subscribed to producer: " + producerId);
                    break;
                }
            }
        }
    }

    // helper method to get all topics:
    public Map<String, Topic<T>> getTopics() {
        return topics;
    }

    // help method to get single topic:
    public Topic<T> getTopic(String topicId) {
        return topics.get(topicId);
    }

    // helper method to get all consumer groups:
    public Map<String, ConsumerGroup<T>> getConsumerGroups() {
        return consumerGroups;
    }

    // helper method to get all producers:
    public Map<String, Producer<T>> getProducers() {
        return producers;
    }
    
}
