package com.webcharm.backend.api;

import com.webcharm.backend.eventtype.api.EventController;
import com.webcharm.backend.eventtype.api.PayloadSchemaValidator;
import com.webcharm.backend.eventtype.crypto.PayloadEncryptionService;
import com.webcharm.backend.eventtype.storage.ImageUploadService;
import com.webcharm.backend.kafka.EventProducer;
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
import org.springframework.mock.web.MockMultipartFile;

import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Verifies the pre-parse request-body size limit: a Content-Length above MAX_REQUEST_CONTENT_LENGTH
 * is rejected with 413 before the body is deserialized, so an oversized body never reaches the
 * controller or the Kafka producer. The cap is set low here to keep test payloads small.
 */
@WebMvcTest(EventController.class)
@Import({PayloadSchemaValidator.class, PayloadEncryptionService.class})
@TestPropertySource(properties = {
    "IMAGE_URL_ALLOWED_HOSTS=cdn.example.com",
    "MAX_REQUEST_CONTENT_LENGTH=80"
})
class RequestSizeLimitFilterTest {

    @Autowired MockMvc mvc;
    @MockitoBean EventProducer eventProducer;
    @MockitoBean ImageUploadService imageUploadService;

    @Test
    void requestBodyOverCap_returns413AndNeverPublishes() throws Exception {
        String oversized = "{\"eventType\":\"DATA\",\"payload\":{\"k\":\""
            + "x".repeat(200) + "\"}}";

        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content(oversized))
            .andExpect(status().isPayloadTooLarge());

        verify(eventProducer, never()).send(any(), any(), any());
    }

    @Test
    void multipartUploadOverCap_isNotRejectedByByteCap() throws Exception {
        when(imageUploadService.upload(any(), any(), any()))
            .thenReturn("images/2026-05-16/test-id.jpg");

        MockMultipartFile file = new MockMultipartFile(
                "file", "photo.jpg", "image/jpeg", "x".repeat(500).getBytes());

        mvc.perform(multipart("/api/events/image-upload").file(file))
            .andExpect(status().isOk());
    }

    @Test
    void requestBodyUnderCap_passesThrough() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"DATA\",\"payload\":{\"k\":\"v\"}}"))
            .andExpect(status().isOk());

        verify(eventProducer).send(any(), any(), any());
    }
}
