package com.webcharm.backend.api;

import com.webcharm.backend.kafka.EventProducer;
import com.webcharm.backend.kafka.KafkaPublishException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;

import java.io.IOException;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Verifies HTTP routing and request validation.
 * Uses Spring's @WebMvcTest, which loads only the web layer (controllers, filters, validation) without starting a real server, database, or Kafka broker.
 * EventProducer is replaced by a Mockito stub (@MockBean): when the controller calls producer.send(),
 * the stub silently records the call instead of connecting to Kafka.
 * No message is ever produced or sent anywhere — the tests only assert that the controller invoked send() with the right arguments.
 */
@WebMvcTest(EventController.class)
class EventControllerTest {

    @Autowired
    MockMvc mvc;

    @MockitoBean
    EventProducer eventProducer;

    @Test
    void publishEvent_validDataEvent_returns200AndPublishes() throws Exception {
        String body = """
                {"eventType":"DATA","payload":{"key":"value"}}
                """;

        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.id").exists())
            .andExpect(jsonPath("$.eventTime").exists());

        verify(eventProducer).send(argThat(e -> "DATA".equals(e.get("eventType"))));
    }

    @Test
    void publishEvent_blankEventType_returns400() throws Exception {
        String body = """
                {"eventType":""}
                """;

        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(body))
            .andExpect(status().isBadRequest());
    }

    @Test
    void publishEvent_missingEventType_returns400() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{}"))
            .andExpect(status().isBadRequest());
    }

    @Test
    void publishImageUpload_validFile_returns200AndPublishes() throws Exception {
        MockMultipartFile file = new MockMultipartFile(
                "file", "photo.jpg", "image/jpeg", new byte[]{1, 2, 3});

        mvc.perform(multipart("/api/events/image-upload").file(file))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.id").exists());

        verify(eventProducer)
                .send(argThat(e -> "IMAGE".equals(e.get("eventType")) && e.containsKey("imageBase64")));
    }

    @Test
    void publishEvent_kafkaSendFailure_returns503() throws Exception {
        doThrow(new KafkaPublishException("broker down", new RuntimeException()))
                .when(eventProducer).send(any());

        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"DATA\"}"))
            .andExpect(status().isServiceUnavailable());
    }

    @Test
    void publishImageUpload_emptyFile_returns400() throws Exception {
        MockMultipartFile file = new MockMultipartFile(
                "file", "empty.jpg", "image/jpeg", new byte[0]);

        mvc.perform(multipart("/api/events/image-upload").file(file))
            .andExpect(status().isBadRequest());
    }

    @Test
    void publishImageUpload_ioError_returns500() throws Exception {
        MockMultipartFile file = new MockMultipartFile(
                "file", "broken.jpg", "image/jpeg", new byte[]{1}) {
            @Override
            public byte[] getBytes() throws IOException {
                throw new IOException("disk read failed");
            }
        };

        mvc.perform(multipart("/api/events/image-upload").file(file))
            .andExpect(status().isInternalServerError());
    }
}
