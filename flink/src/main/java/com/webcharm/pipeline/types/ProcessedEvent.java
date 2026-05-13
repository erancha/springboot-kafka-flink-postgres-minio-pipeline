package com.webcharm.pipeline.types;

import java.io.Serializable;
import java.time.Instant;
import java.time.LocalDate;
import java.util.Map;
import java.util.UUID;

public class ProcessedEvent implements Serializable {
    private UUID id;
    private String eventType;
    private Instant eventTime;
    private String source;
    private Map<String, Object> payload;
    private String imageUrl;
    private String imageBase64;
    private String imageContentType;
    private String imageObjectKey;
    private LocalDate date;

    public ProcessedEvent() {
    }

    public ProcessedEvent(
            UUID id,
            String eventType,
            Instant eventTime,
            String source,
            Map<String, Object> payload,
            String imageUrl,
            String imageBase64,
            String imageContentType,
            String imageObjectKey,
            LocalDate date) {
        this.id = id;
        this.eventType = eventType;
        this.eventTime = eventTime;
        this.source = source;
        this.payload = payload;
        this.imageUrl = imageUrl;
        this.imageBase64 = imageBase64;
        this.imageContentType = imageContentType;
        this.imageObjectKey = imageObjectKey;
        this.date = date;
    }

    public UUID getId() {
        return id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Instant getEventTime() {
        return eventTime;
    }

    public void setEventTime(Instant eventTime) {
        this.eventTime = eventTime;
    }

    public String getSource() {
        return source;
    }

    public void setSource(String source) {
        this.source = source;
    }

    public Map<String, Object> getPayload() {
        return payload;
    }

    public void setPayload(Map<String, Object> payload) {
        this.payload = payload;
    }

    public String getImageUrl() {
        return imageUrl;
    }

    public void setImageUrl(String imageUrl) {
        this.imageUrl = imageUrl;
    }

    public String getImageBase64() {
        return imageBase64;
    }

    public void setImageBase64(String imageBase64) {
        this.imageBase64 = imageBase64;
    }

    public String getImageContentType() {
        return imageContentType;
    }

    public void setImageContentType(String imageContentType) {
        this.imageContentType = imageContentType;
    }

    public String getImageObjectKey() {
        return imageObjectKey;
    }

    public void setImageObjectKey(String imageObjectKey) {
        this.imageObjectKey = imageObjectKey;
    }

    public LocalDate getDate() {
        return date;
    }

    public void setDate(LocalDate date) {
        this.date = date;
    }
}
