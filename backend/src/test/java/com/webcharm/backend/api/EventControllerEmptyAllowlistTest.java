package com.webcharm.backend.api;

import com.webcharm.backend.kafka.EventProducer;
import com.webcharm.backend.storage.ImageUploadService;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Verifies that IMAGE events with imageUrl are denied when IMAGE_URL_ALLOWED_HOSTS is not configured.
 * Kept separate from EventControllerTest because the allowlist property is class-scoped in @WebMvcTest.
 */
@WebMvcTest(EventController.class)
class EventControllerEmptyAllowlistTest {

    @Autowired MockMvc mvc;
    @MockitoBean EventProducer eventProducer;
    @MockitoBean ImageUploadService imageUploadService;

    @Test
    void publishEvent_imageEventWithUnconfiguredAllowlist_returns403() throws Exception {
        mvc.perform(post("/api/events")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"eventType\":\"IMAGE\",\"imageUrl\":\"https://example.com/photo.jpg\"}"))
            .andExpect(status().isForbidden());
    }
}
