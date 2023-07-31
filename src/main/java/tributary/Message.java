package tributary;

import java.time.LocalDateTime;

public class Message<T> {
    private LocalDateTime dateTimeCreated;
    private String Id;
    private String payloadType;
    private String key;
    private T value;

    public Message(LocalDateTime dateTimeCreated, String id, String payloadType, String key, T value) {
        this.dateTimeCreated = dateTimeCreated;
        this.Id = id;
        this.payloadType = payloadType;
        this.key = key;
        this.value = value;
    }

    public LocalDateTime getDateTimeCreated() {
        return dateTimeCreated;
    }

    public void setDateTimeCreated(LocalDateTime dateTimeCreated) {
        this.dateTimeCreated = dateTimeCreated;
    }

    public String getId() {
        return Id;
    }

    public void setId(String id) {
        this.Id = id;
    }

    public String getPayloadType() {
        return payloadType;
    }

    public void setPayloadType(String payloadType) {
        this.payloadType = payloadType;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }
}