package org.example.pravega.client.batchclient.embeddedinproccluster;

import com.google.common.collect.Lists;
import io.pravega.client.BatchClientFactory;
import io.pravega.client.ClientConfig;
import io.pravega.client.EventStreamClientFactory;
import io.pravega.client.admin.StreamManager;
import io.pravega.client.batch.SegmentIterator;
import io.pravega.client.batch.SegmentRange;
import io.pravega.client.control.impl.ControllerImpl;
import io.pravega.client.stream.EventRead;
import io.pravega.client.stream.EventStreamReader;
import io.pravega.client.stream.EventStreamWriter;
import io.pravega.client.stream.EventWriterConfig;
import io.pravega.client.stream.ReaderConfig;
import io.pravega.client.stream.ReinitializationRequiredException;
import io.pravega.client.stream.ScalingPolicy;
import io.pravega.client.stream.Stream;
import io.pravega.client.stream.StreamConfiguration;
import io.pravega.client.stream.impl.JavaSerializer;
import io.pravega.common.hash.RandomFactory;
import lombok.Cleanup;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.pravega.shared.NameUtils.computeSegmentId;
import static java.util.stream.Collectors.toList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

@Slf4j
class BatchClientTestHelper {

    private static final int READ_TIMEOUT = 1000;

    static final String DATA_OF_SIZE_30 = "data of size 30"; // data length = 22 bytes , header = 8 bytes

    private static final Random random = RandomFactory.create();



    /*
     * Create a test stream with 1 segment which is scaled-up to 3 segments and later scaled-down to 2 segments.
     * Events of constant size are written to the stream before and after scale operation.
     */
    static void createTestStreamWithEvents(ClientConfig clientConfig, String scopeName, String streamName,
                                           ControllerImpl controller,
                                           ScheduledExecutorService executor) throws ExecutionException, InterruptedException {

        @Cleanup
        StreamManager streamManager = StreamManager.create(clientConfig);
        log.info("Created a stream manager");

        // Create the scope
        streamManager.createScope(scopeName);
        log.info("Created a scope [{}]", scopeName);

        // Create a stream with 1 segment
        streamManager.createStream(scopeName, streamName,
                StreamConfiguration.builder().scalingPolicy(ScalingPolicy.fixed(1)).build());
        log.info("Created a stream with name [{}]", streamName);


        @Cleanup
        EventStreamClientFactory clientFactory = EventStreamClientFactory.withScope(scopeName, clientConfig);
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<String>(),
                EventWriterConfig.builder().build());

        addEventsToStream(writer, scopeName, streamName, controller, executor);

    }

    static void addEventsToStream(EventStreamWriter<String> writer, String scopeName,
                                           String streamName,
                                           ControllerImpl controller,
                                           ScheduledExecutorService executor) throws ExecutionException, InterruptedException {

        // write 3 30 byte events to the test stream with 1 segment.
        write30ByteEvents(3, writer);
        log.info("Wrote 3 events, while the stream had 1 segment.");

        Stream stream = Stream.of(scopeName, streamName);

        /* scale the stream up to 3 segments and then write events. */

        Map<Double, Double> map = new HashMap<>();
        map.put(0.0, 0.33);
        map.put(0.33, 0.66);
        map.put(0.66, 1.0);
        boolean scaleResult = controller.scaleStream(stream, Collections.singletonList(0L), map, executor)
                .getFuture()
                .get();
        if (!scaleResult) {
            throw new RuntimeException("Failed to scale the stream");
        }
        log.info("Done scaling up the stream to 3 segments. Scale result was {}", scaleResult);

        write30ByteEvents(3, writer);
        log.info("Wrote 3 more events.");

        /* Scale the stream down to 2 segments, and then write some more events */

        map = new HashMap<>();
        map.put(0.0, 0.5);
        map.put(0.5, 1.0);

        scaleResult = controller.scaleStream(stream,
                Arrays.asList(computeSegmentId(1, 1),
                        computeSegmentId(2, 1),
                        computeSegmentId(3, 1)), map, executor).getFuture().get();
        log.info("Done scaling down the stream to 2 segments. Scale result was {}", scaleResult);
        if (!scaleResult) {
            throw new RuntimeException("Failed to scale the stream");
        }
        write30ByteEvents(3, writer);
        log.info("Wrote 3 more events.");
    }

    static void writeEvents(EventStreamClientFactory clientFactory, String streamName, int totalEvents) {
        writeEvents(clientFactory, streamName, totalEvents, 0);
    }

    static void writeEvents(EventStreamClientFactory clientFactory, String streamName, int totalEvents, int initialPoint) {
        @Cleanup
        EventStreamWriter<String> writer = clientFactory.createEventWriter(streamName, new JavaSerializer<>(),
                EventWriterConfig.builder().build());
        for (int i = initialPoint; i < totalEvents + initialPoint; i++) {
            writer.writeEvent(String.valueOf(i)).join();
            log.debug("Writing event: {} to stream {}.", streamName + String.valueOf(i), streamName);
        }
    }

    static int readFromRanges(List<SegmentRange> ranges, BatchClientFactory batchClient) {
        List<CompletableFuture<Integer>> eventCounts = ranges
                .parallelStream()
                .map(range -> CompletableFuture.supplyAsync(() -> batchClient.readSegment(range, new JavaSerializer<>()))
                        .thenApplyAsync(segmentIterator -> {
                            log.debug("Thread " + Thread.currentThread().getId() + " reading events.");
                            int numEvents = Lists.newArrayList(segmentIterator).size();
                            segmentIterator.close();
                            return numEvents;
                        }))
                .collect(Collectors.toList());
        return eventCounts.stream().map(CompletableFuture::join).mapToInt(Integer::intValue).sum();
    }

    static <T extends Serializable> List<CompletableFuture<Integer>> readEventFutures(EventStreamClientFactory client, String rGroup, int numReaders, int limit) {
        List<EventStreamReader<T>> readers = new ArrayList<>();
        for (int i = 0; i < numReaders; i++) {
            readers.add(client.createReader(rGroup + "-" + String.valueOf(i), rGroup,
                    new JavaSerializer<>(), ReaderConfig.builder().build()));
        }

        return readers.stream().map(r -> CompletableFuture.supplyAsync(() ->
                readEvents(r, limit / numReaders))).collect(toList());
    }

    static List<CompletableFuture<Integer>> readEventFutures(EventStreamClientFactory clientFactory, String readerGroup, int numReaders) {
        return readEventFutures(clientFactory, readerGroup, numReaders, Integer.MAX_VALUE);
    }

    static void write30ByteEvents(int numberOfEvents, EventStreamWriter<String> writer) {
        Supplier<String> routingKeyGenerator = () -> String.valueOf(random.nextInt());
        IntStream.range(0, numberOfEvents).forEach(v -> writer.writeEvent(routingKeyGenerator.get(),
                DATA_OF_SIZE_30).join());
    }

    static <T> int readEvents(EventStreamReader<T> reader, int limit) {
        return readEvents(reader, limit, false);
    }

    static <T> int readEvents(EventStreamReader<T> reader, int limit, boolean reinitializationExpected) {
        EventRead<T> event = null;
        int validEvents = 0;
        boolean reinitializationRequired = false;
        try {
            do {
                try {
                    event = reader.readNextEvent(READ_TIMEOUT);
                    log.debug("Read event result in readEvents: {}.", event.getEvent());
                    if (event.getEvent() != null) {
                        validEvents++;
                    }
                    reinitializationRequired = false;
                } catch (ReinitializationRequiredException e) {
                    log.error("Exception while reading event using readerId: {}", reader, e);
                    if (reinitializationExpected) {
                        reinitializationRequired = true;
                    } else {
                        fail("Reinitialization Exception is not expected");
                    }
                }
            } while (reinitializationRequired || ((event.getEvent() != null || event.isCheckpoint()) && validEvents < limit));
        } finally {
            closeReader(reader);
        }

        return validEvents;
    }

    static <T> void closeReader(EventStreamReader<T> reader) {
        try {
            log.info("Closing reader");
            reader.close();
        } catch (Throwable e) {
            log.error("Error while closing reader", e);
        }
    }

    static void validateSegmentCountAndEventCount(BatchClientFactory batchClient,
                                                  ArrayList<SegmentRange> segmentsPostTruncation) {
        //expected segments = 1+ 3 + 2 = 6
        assertEquals("Expected number of segments post truncation", 6, segmentsPostTruncation.size());
        List<String> eventsPostTruncation = new ArrayList<>();
        segmentsPostTruncation.forEach(segInfo -> {
            @Cleanup
            SegmentIterator<String> segmentIterator = batchClient.readSegment(segInfo, new JavaSerializer<String>());
            eventsPostTruncation.addAll(Lists.newArrayList(segmentIterator));
        });
        assertEquals("Event count post truncation", 7, eventsPostTruncation.size());
    }
}
