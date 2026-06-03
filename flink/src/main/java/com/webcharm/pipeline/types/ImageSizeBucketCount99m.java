package com.webcharm.pipeline.types;

import java.io.Serializable;
import java.time.Instant;

public class ImageSizeBucketCount99m implements Serializable {
  private Instant windowStart;
  private Instant windowEnd;
  private String bucket;
  private long imageCount;

  public ImageSizeBucketCount99m() {
  }

  public ImageSizeBucketCount99m(Instant windowStart, Instant windowEnd, String bucket, long imageCount) {
    this.windowStart = windowStart;
    this.windowEnd = windowEnd;
    this.bucket = bucket;
    this.imageCount = imageCount;
  }

  public Instant getWindowStart() {
    return windowStart;
  }

  public void setWindowStart(Instant windowStart) {
    this.windowStart = windowStart;
  }

  public Instant getWindowEnd() {
    return windowEnd;
  }

  public void setWindowEnd(Instant windowEnd) {
    this.windowEnd = windowEnd;
  }

  public String getBucket() {
    return bucket;
  }

  public void setBucket(String bucket) {
    this.bucket = bucket;
  }

  public long getImageCount() {
    return imageCount;
  }

  public void setImageCount(long imageCount) {
    this.imageCount = imageCount;
  }
}
