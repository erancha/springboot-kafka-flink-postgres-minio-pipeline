package com.webcharm.backend.eventtype.api;

import com.webcharm.backend.kafka.EventProducer;
import com.webcharm.backend.eventtype.crypto.PayloadEncryptionService;
import com.webcharm.backend.eventtype.storage.ImageUploadService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Verifies DATA payloads are validated against the shipped schema (event-payload-schema.json,
 * the .env.example default), an order record requiring orderNumber, quantity, and sku: a conforming
 * payload publishes, while unknown keys, missing required keys, and wrong types are rejected with
 * 400 and never reach Kafka.
 */
@WebMvcTest(EventController.class)
@Import({PayloadSchemaValidator.class, PayloadEncryptionService.class})
@TestPropertySource(properties = {
    "IMAGE_URL_ALLOWED_HOSTS=cdn.example.com",
    "EVENT_PAYLOAD_SCHEMA=classpath:eventtype/event-payload-schema.json"
})
class EventControllerSchemaTest {

    @Autowired MockMvc mvc;
    @MockitoBean EventProducer eventProducer;
    @MockitoBean ImageUploadService imageUploadService;

    @Test
    void conformingPayload_returns200AndPublishes() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"DATA\",\"payload\":{\"orderNumber\":\"ORD-1001\",\"quantity\":3,\"sku\":\"SKU-42\"}}"))
            .andExpect(status().isOk());

        verify(eventProducer).send(any(), any(), any());
    }

    @Test
    void unknownKey_returns400AndDoesNotPublish() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"DATA\",\"payload\":{\"orderNumber\":\"ORD-1001\",\"quantity\":3,\"rogue\":true}}"))
            .andExpect(status().isBadRequest());

        verify(eventProducer, never()).send(any(), any(), any());
    }

    @Test
    void missingRequiredKey_returns400AndDoesNotPublish() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"DATA\",\"payload\":{\"orderNumber\":\"ORD-1001\"}}"))
            .andExpect(status().isBadRequest());

        verify(eventProducer, never()).send(any(), any(), any());
    }

    @Test
    void wrongType_returns400AndDoesNotPublish() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"DATA\",\"payload\":{\"orderNumber\":\"ORD-1001\",\"quantity\":\"three\"}}"))
            .andExpect(status().isBadRequest());

        verify(eventProducer, never()).send(any(), any(), any());
    }

    @Test
    void imageEventBypassesPayloadSchema_returns200() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"IMAGE\",\"imageUrl\":\"https://cdn.example.com/p.jpg\"}"))
            .andExpect(status().isOk());
    }
}
