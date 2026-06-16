package com.webcharm.pipeline.eventtype;

import com.webcharm.pipeline.eventtype.types.EventTypeCountAgg;
import com.webcharm.pipeline.eventtype.types.ImageSizeBucket;
import com.webcharm.pipeline.eventtype.types.ImageSizeBucketCountAgg;
import com.webcharm.pipeline.eventtype.types.ProcessedEvent;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Verifies the tumbling event-time window aggregations (5-minute per-type counts, 10-minute
 * image-size buckets) using a local Flink environment and a bounded in-memory source with
 * controlled timestamps — no waiting required.
 */
class StreamingJobIT {

    @Test
    void windowedCounts_twoEventTypes_emitsCorrectCountsPerTypeAndWindow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        Instant base = Instant.parse("2024-01-01T00:00:00Z");

        // 3 DATA + 2 IMAGE all in the [00:00, 00:05) window.
        // The trigger at 00:10:10 advances the watermark to 00:10:00, which is past the
        // [00:00, 00:05) window end — causing that window to fire immediately.
        DataStream<ProcessedEvent> source = env
            .fromData(
                event("DATA",  base.plusSeconds(60)),   // 00:01
                event("DATA",  base.plusSeconds(120)),  // 00:02
                event("DATA",  base.plusSeconds(180)),  // 00:03
                event("IMAGE", base.plusSeconds(60)),   // 00:01
                event("IMAGE", base.plusSeconds(120)),  // 00:02
                event("DATA",  base.plusSeconds(610))   // 00:10:10 — watermark trigger
            )
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<ProcessedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withTimestampAssigner((e, t) -> e.getEventTime().toEpochMilli()))
            .name("test-source");

        List<EventTypeCountAgg> results = new ArrayList<>();
        try (CloseableIterator<EventTypeCountAgg> it =
                StreamingJob.buildEventTypeCounts(source).executeAndCollect()) {
            it.forEachRemaining(results::add);
        }

        Map<String, Long> window1 = results.stream()
            .filter(r -> r.getWindowStart().equals(base))
            .collect(Collectors.toMap(EventTypeCountAgg::getEventType, EventTypeCountAgg::getEventCount));

        assertEquals(3L, window1.get("DATA"),  "DATA count in [00:00, 00:05)");
        assertEquals(2L, window1.get("IMAGE"), "IMAGE count in [00:00, 00:05)");
    }

    @Test
    void imageSizeBuckets_emitsCountPerBucketAndWindow() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
        env.setParallelism(1);

        Instant base = Instant.parse("2024-01-01T00:00:00Z");

        // The bucket is carried in the source field purely so this bounded source can reuse the
        // proven ProcessedEvent timestamp/watermark setup; the last sample at 00:10:10 only
        // advances the watermark to fire the [00:00, 00:10) window.
        DataStream<ImageSizeBucket> samples = env
            .fromData(
                bucketEvent(ImageSizeBucket.UP_TO_1MB, base.plusSeconds(60)),   // 00:01
                bucketEvent(ImageSizeBucket.UP_TO_1MB, base.plusSeconds(120)),  // 00:02
                bucketEvent(ImageSizeBucket.UP_TO_5MB, base.plusSeconds(180)),  // 00:03
                bucketEvent(ImageSizeBucket.UP_TO_1MB, base.plusSeconds(610))   // 00:10:10 — trigger
            )
            .assignTimestampsAndWatermarks(
                WatermarkStrategy
                    .<ProcessedEvent>forBoundedOutOfOrderness(Duration.ofSeconds(10))
                    .withTimestampAssigner((e, t) -> e.getEventTime().toEpochMilli()))
            .map(e -> ImageSizeBucket.valueOf(e.getSource()))
            .returns(TypeInformation.of(ImageSizeBucket.class))
            .name("test-size-buckets");

        List<ImageSizeBucketCountAgg> results = new ArrayList<>();
        try (CloseableIterator<ImageSizeBucketCountAgg> it =
                StreamingJob.buildImageSizeBuckets(samples).executeAndCollect()) {
            it.forEachRemaining(results::add);
        }

        Map<String, Long> window1 = results.stream()
            .filter(r -> r.getWindowStart().equals(base))
            .collect(Collectors.toMap(ImageSizeBucketCountAgg::getBucket, ImageSizeBucketCountAgg::getImageCount));

        assertEquals(2L, window1.get("<=1MB"), "<=1MB count in [00:00, 00:10)");
        assertEquals(1L, window1.get("<=5MB"), "<=5MB count in [00:00, 00:10)");
    }

    private static ProcessedEvent event(String type, Instant time) {
        return new ProcessedEvent(
            UUID.randomUUID(), type, time, "test",
            null, null, null,
            time.atZone(ZoneOffset.UTC).toLocalDate());
    }

    private static ProcessedEvent bucketEvent(ImageSizeBucket bucket, Instant time) {
        return new ProcessedEvent(
            UUID.randomUUID(), "IMAGE", time, bucket.name(),
            null, null, null,
            time.atZone(ZoneOffset.UTC).toLocalDate());
    }
}
