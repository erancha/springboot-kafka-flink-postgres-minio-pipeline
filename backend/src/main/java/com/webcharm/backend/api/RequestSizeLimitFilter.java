package com.webcharm.backend.api;

import jakarta.servlet.FilterChain;
import jakarta.servlet.ServletException;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 * Rejects non-multipart /api requests whose Content-Length exceeds MAX_REQUEST_CONTENT_LENGTH
 * with 413; multipart bodies are bounded by Spring's multipart limits instead.
 */
@Component
public class RequestSizeLimitFilter extends OncePerRequestFilter {

  private static final Logger log = LoggerFactory.getLogger(RequestSizeLimitFilter.class);

  private final long maxBytes;

  // Boxed Long, not long, so the documented "empty disables" value binds to null (Spring maps an
  // empty property to null) rather than failing startup. Null and the absent-property default both
  // mean 0, which shouldNotFilter treats as "no limit".
  public RequestSizeLimitFilter(@Value("${MAX_REQUEST_CONTENT_LENGTH:0}") Long maxBytes) {
    this.maxBytes = Objects.requireNonNullElse(maxBytes, 0L);
  }

  @Override
  protected boolean shouldNotFilter(HttpServletRequest request) {
    if (maxBytes <= 0 || !request.getRequestURI().startsWith("/api/")) {
      return true;
    }
    String contentType = request.getContentType();
    return contentType != null && contentType.toLowerCase().startsWith("multipart/");
  }

  @Override
  protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response,
      FilterChain chain) throws ServletException, IOException {
    // -1 when no Content-Length header (chunked) — such requests are not bounded here.
    long contentLength = request.getContentLengthLong();
    if (contentLength > maxBytes) {
      log.debug("Rejected {} {}: Content-Length {} exceeds {} bytes",
          request.getMethod(), request.getRequestURI(), contentLength, maxBytes);
      response.setStatus(HttpStatus.PAYLOAD_TOO_LARGE.value());
      response.setContentType("text/plain");
      response.getWriter().write("Request body exceeds " + maxBytes + " bytes");
      return;
    }
    chain.doFilter(request, response);
  }
}
