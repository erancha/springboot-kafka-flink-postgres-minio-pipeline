package com.webcharm.backend.userkeys.api;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import com.webcharm.backend.kafka.EventProducer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.http.MediaType;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.web.servlet.MockMvc;

/**
 * Verifies HTTP routing, request validation, and the Kafka publish for UserKeyController.
 * Uses @WebMvcTest (web layer only); EventProducer is a Mockito stub.
 */
@WebMvcTest(UserKeyController.class)
@TestPropertySource(properties = "app.userkeys.kafka.topic=user-keys")
class UserKeyControllerTest {

  @Autowired MockMvc mvc;
  @MockitoBean EventProducer eventProducer;

  @Test
  void publishUserKey_validEvent_returns200AndPublishesToTopic() throws Exception {
    mvc.perform(post("/api/user-keys")
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"userId\":\"u-1\",\"key\":\"A\",\"value\":250}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.id").exists())
        .andExpect(jsonPath("$.eventTime").exists());

    verify(eventProducer).send(eq("user-keys"), eq("u-1|A"),
        argThat(e -> "u-1".equals(e.get("userId")) && "A".equals(e.get("key"))
            && Long.valueOf(250L).equals(e.get("value"))));
  }

  @Test
  void publishUserKey_missingUserId_returns400AndDoesNotPublish() throws Exception {
    mvc.perform(post("/api/user-keys")
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"key\":\"A\",\"value\":1}"))
        .andExpect(status().isBadRequest());
    verify(eventProducer, never()).send(any(), any(), any());
  }

  @Test
  void publishUserKey_blankKey_returns400AndDoesNotPublish() throws Exception {
    mvc.perform(post("/api/user-keys")
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"userId\":\"u-1\",\"key\":\"  \",\"value\":1}"))
        .andExpect(status().isBadRequest());
    verify(eventProducer, never()).send(any(), any(), any());
  }

  @Test
  void publishUserKey_missingValue_returns400AndDoesNotPublish() throws Exception {
    mvc.perform(post("/api/user-keys")
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"userId\":\"u-1\",\"key\":\"A\"}"))
        .andExpect(status().isBadRequest());
    verify(eventProducer, never()).send(any(), any(), any());
  }

  @Test
  void publishUserKey_nonNumericValue_returns400AndDoesNotPublish() throws Exception {
    mvc.perform(post("/api/user-keys")
            .contentType(MediaType.APPLICATION_JSON)
            .content("{\"userId\":\"u-1\",\"key\":\"A\",\"value\":\"not-a-number\"}"))
        .andExpect(status().isBadRequest());
    verify(eventProducer, never()).send(any(), any(), any());
  }
}
