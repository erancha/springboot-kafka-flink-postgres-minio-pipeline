package com.webcharm.backend.api;

import com.webcharm.backend.kafka.EventProducer;
import com.webcharm.backend.kafka.KafkaPublishException;
import com.webcharm.backend.storage.ImageUploadService;
import com.webcharm.backend.storage.ObjectStoreException;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.http.MediaType;
import org.springframework.mock.web.MockMultipartFile;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.multipart;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Verifies HTTP routing, request validation, and imageUrl SSRF guard for EventController.
 * Uses @WebMvcTest (web layer only). EventProducer and ImageUploadService are Mockito stubs.
 * IMAGE_URL_ALLOWED_HOSTS is set to "cdn.example.com" so imageUrl tests can verify allowed vs blocked hosts.
 */
@WebMvcTest(EventController.class)
@TestPropertySource(properties = "IMAGE_URL_ALLOWED_HOSTS=cdn.example.com")
class EventControllerTest {

    @Autowired MockMvc mvc;
    @MockitoBean EventProducer eventProducer;
    @MockitoBean ImageUploadService imageUploadService;

    @Test
    void publishEvent_validDataEvent_returns200AndPublishes() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"DATA\",\"payload\":{\"key\":\"value\"}}"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.id").exists())
            .andExpect(jsonPath("$.eventTime").exists());

        verify(eventProducer).send(argThat(e -> "DATA".equals(e.get("eventType"))));
    }

    @Test
    void publishEvent_textEventType_normalizedToDataAndPublished() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"TEXT\",\"payload\":{\"msg\":\"hello\"}}"))
            .andExpect(status().isOk());

        verify(eventProducer).send(argThat(e -> "DATA".equals(e.get("eventType"))));
    }

    @Test
    void publishEvent_unknownEventType_returns400() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"BANANA\"}"))
            .andExpect(status().isBadRequest());
    }

    @Test
    void publishEvent_blankEventType_returns400() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"\"}"))
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
    void publishImageUpload_validFile_returns200AndPublishesPointerEvent() throws Exception {
        when(imageUploadService.upload(any(), any(), any()))
            .thenReturn("images/2026-05-16/test-id.jpg");

        MockMultipartFile file = new MockMultipartFile(
                "file", "photo.jpg", "image/jpeg", new byte[]{1, 2, 3});

        mvc.perform(multipart("/api/events/image-upload").file(file))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.id").exists());

        verify(eventProducer).send(argThat(e ->
            "IMAGE".equals(e.get("eventType")) && e.containsKey("imageObjectKey")));
    }

    @Test
    void publishImageUpload_emptyFile_returns400() throws Exception {
        MockMultipartFile file = new MockMultipartFile(
                "file", "empty.jpg", "image/jpeg", new byte[0]);

        mvc.perform(multipart("/api/events/image-upload").file(file))
            .andExpect(status().isBadRequest());
    }

    @Test
    void publishImageUpload_nonImageContentType_returns400AndDoesNotPublish() throws Exception {
        MockMultipartFile file = new MockMultipartFile(
                "file", "doc.pdf", "application/pdf", new byte[]{1, 2, 3});

        mvc.perform(multipart("/api/events/image-upload").file(file))
            .andExpect(status().isBadRequest());

        verify(eventProducer, never()).send(any());
        verify(imageUploadService, never()).upload(any(), any(), any());
    }

    @Test
    void publishImageUpload_missingContentType_returns400AndDoesNotPublish() throws Exception {
        MockMultipartFile file = new MockMultipartFile(
                "file", "mystery.bin", null, new byte[]{1, 2, 3});

        mvc.perform(multipart("/api/events/image-upload").file(file))
            .andExpect(status().isBadRequest());

        verify(eventProducer, never()).send(any());
        verify(imageUploadService, never()).upload(any(), any(), any());
    }

    @Test
    void publishImageUpload_oversizedFile_returns400AndDoesNotPublish() throws Exception {
        byte[] tooBig = new byte[10 * 1024 * 1024 + 1];
        MockMultipartFile file = new MockMultipartFile(
                "file", "huge.jpg", "image/jpeg", tooBig);

        mvc.perform(multipart("/api/events/image-upload").file(file))
            .andExpect(status().isBadRequest());

        verify(eventProducer, never()).send(any());
        verify(imageUploadService, never()).upload(any(), any(), any());
    }

    @Test
    void publishImageUpload_ioError_returns500() throws Exception {
        when(imageUploadService.upload(any(), any(), any()))
            .thenThrow(new IOException("disk read failed"));

        MockMultipartFile file = new MockMultipartFile(
                "file", "broken.jpg", "image/jpeg", new byte[]{1});

        mvc.perform(multipart("/api/events/image-upload").file(file))
            .andExpect(status().isInternalServerError());
    }

    @Test
    void publishImageUpload_objectStoreFailure_returns503() throws Exception {
        when(imageUploadService.upload(any(), any(), any()))
            .thenThrow(new ObjectStoreException("object store unavailable", new RuntimeException()));

        MockMultipartFile file = new MockMultipartFile(
                "file", "photo.jpg", "image/jpeg", new byte[]{1, 2, 3});

        mvc.perform(multipart("/api/events/image-upload").file(file))
            .andExpect(status().isServiceUnavailable());
    }

    @Test
    void publishImageUpload_kafkaFailure_deletesOrphanedObjectAndReturns503() throws Exception {
        when(imageUploadService.upload(any(), any(), any()))
            .thenReturn("images/2026-05-16/test-id.jpg");
        doThrow(new KafkaPublishException("broker down", new RuntimeException()))
            .when(eventProducer).send(any());

        MockMultipartFile file = new MockMultipartFile(
                "file", "photo.jpg", "image/jpeg", new byte[]{1, 2, 3});

        mvc.perform(multipart("/api/events/image-upload").file(file))
            .andExpect(status().isServiceUnavailable());

        verify(imageUploadService).delete("images/2026-05-16/test-id.jpg");
    }

    @Test
    void publishImageUpload_kafkaFailureAndCleanupFails_stillReturns503() throws Exception {
        when(imageUploadService.upload(any(), any(), any()))
            .thenReturn("images/2026-05-16/test-id.jpg");
        doThrow(new KafkaPublishException("broker down", new RuntimeException()))
            .when(eventProducer).send(any());
        doThrow(new ObjectStoreException("delete failed", new RuntimeException()))
            .when(imageUploadService).delete(any());

        MockMultipartFile file = new MockMultipartFile(
                "file", "photo.jpg", "image/jpeg", new byte[]{1, 2, 3});

        mvc.perform(multipart("/api/events/image-upload").file(file))
            .andExpect(status().isServiceUnavailable());
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

    // ── imageUrl SSRF guard ───────────────────────────────────────────────────

    @Test
    void publishEvent_imageEventWithAllowedHost_returns200() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"IMAGE\",\"imageUrl\":\"https://cdn.example.com/photo.jpg\"}"))
            .andExpect(status().isOk());
    }

    @Test
    void publishEvent_imageEventWithBlockedHost_returns403() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"IMAGE\",\"imageUrl\":\"https://blocked.com/photo.jpg\"}"))
            .andExpect(status().isForbidden());
    }

    @Test
    void publishEvent_imageEventWithFileScheme_returns400() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"IMAGE\",\"imageUrl\":\"file:///etc/passwd\"}"))
            .andExpect(status().isBadRequest());
    }

    @Test
    void publishEvent_imageEventWithFtpScheme_returns400() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"IMAGE\",\"imageUrl\":\"ftp://cdn.example.com/photo.jpg\"}"))
            .andExpect(status().isBadRequest());
    }

    @Test
    void publishEvent_imageEventWithMalformedUrl_returns400() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"IMAGE\",\"imageUrl\":\"not a valid url\"}"))
            .andExpect(status().isBadRequest());
    }

    @Test
    void publishEvent_imageEventWithNoImageUrl_returns400AndDoesNotPublish() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"IMAGE\"}"))
            .andExpect(status().isBadRequest());

        verify(eventProducer, never()).send(any());
    }

    @Test
    void publishEvent_imageEventWithBlankImageUrl_returns400AndDoesNotPublish() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"IMAGE\",\"imageUrl\":\"   \"}"))
            .andExpect(status().isBadRequest());

        verify(eventProducer, never()).send(any());
    }
}
