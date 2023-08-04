package tributary;

import java.util.HashMap;
import java.util.Map;

public class ProducerFactory<T> {

    private Map<String, Producer<T>> producers = new HashMap<>();

    public Producer<T> getProducer(String id, Class<T> type, Tributary<T> tributary,
    AllocationMethod allocationMethod) {
        if (!producers.containsKey(id)) {
            producers.put(id, new Producer<T>(id, type, tributary, allocationMethod));
        }
        return producers.get(id);
    }
}
