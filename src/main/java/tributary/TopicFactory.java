package tributary;

import java.util.HashMap;
import java.util.Map;

public class TopicFactory<T> {

    private Map<String, Topic<T>> topics = new HashMap<>();

    public Topic<T> getTopic(String id, Class<T> type) {
        if (!topics.containsKey(id)) {
            topics.put(id, new Topic<T>(id, type));
        }
        return topics.get(id);
    }
}
