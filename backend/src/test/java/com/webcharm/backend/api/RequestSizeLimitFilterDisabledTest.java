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
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Pins the documented "empty disables" contract for MAX_REQUEST_CONTENT_LENGTH: an empty value
 * leaves the cap unset rather than failing startup, so the byte-size filter is bypassed and a body
 * that a finite cap would reject is allowed through.
 */
@WebMvcTest(EventController.class)
@Import({PayloadSchemaValidator.class, PayloadEncryptionService.class})
@TestPropertySource(properties = {
    "IMAGE_URL_ALLOWED_HOSTS=cdn.example.com",
    "MAX_REQUEST_CONTENT_LENGTH="
})
class RequestSizeLimitFilterDisabledTest {

    @Autowired MockMvc mvc;
    @MockitoBean EventProducer eventProducer;
    @MockitoBean ImageUploadService imageUploadService;

    @Test
    void emptyCap_disablesLimit_bodyPassesThrough() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"DATA\",\"payload\":{\"k\":\"v\"}}"))
            .andExpect(status().isOk());

        verify(eventProducer).send(any(), any(), any());
    }
}
