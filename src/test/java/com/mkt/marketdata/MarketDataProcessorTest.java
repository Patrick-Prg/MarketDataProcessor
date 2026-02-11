package com.mkt.marketdata;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.*;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
//import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentCaptor;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

class MarketDataProcessorTest {

    // Sample symbols used for the test
    final List<String> symbols = Arrays.asList("A", "AAL", "AAP", "AAPL", "ABBV", "ABC", "ABMD", "ABT", "ACN", "ADBE", "ADI", "ADM", "ADP", "ADSK", "AEE", "AEP", "AES", "AFL", "AIG", "AIV", "AIZ", "AJG", "AKAM", "ALB", "ALGN", "ALK", "ALL", "ALLE", "ALXN", "AMAT", "AMCR", "AMD", "AME", "AMGN", "AMP", "AMT", "AMZN", "ANET", "ANSS", "ANTM", "AON", "AOS", "APA", "APD", "APH", "APTV", "ARE", "ATO", "ATVI", "AVB", "AVGO", "AVY", "AWK", "AXP", "AZO",
            "BA", "BAC", "BAX", "BBY", "BDX", "BEN", "BF", "BIIB", "BIO", "BK", "BKNG", "BKR", "BLK", "BLL", "BMY", "BR", "BRK", "BSX", "BWA", "BXP",
            "C", "CAG", "CAH", "CARR", "CAT", "CB", "CBOE", "CBRE", "CCI", "CCL", "CDNS", "CDW", "CE", "CERN", "CF", "CFG", "CHD", "CHRW", "CHTR", "CI", "CINF", "CL", "CLX", "CMA", "CMCSA");

    private MarketDataProcessor processor;
    private ScheduledExecutorService mockScheduler;
    private TimeProvider fakeTime;
    private Runnable capturedTask;
    private java.util.List<MarketData> publishedEntries = Collections.synchronizedList(new ArrayList<>());

    // Simulate the time ...
    class FakeTimeProvider implements TimeProvider {
        long time = 0L;
        @Override public long currentTimeMillis() { return time; }
        void advance(long ms) { time += ms; }
    }

    @BeforeEach
    void setUp() {
        // Ensure publishedEntries is empty before each test to avoid cross-test contamination
        publishedEntries.clear();

        mockScheduler = mock(ScheduledExecutorService.class);
        fakeTime = new FakeTimeProvider();

        // 1. Initialize a runnable captor to be passed to the scheduler
        //    and capture the scheduled task (tryPublishNextBatch)
        ArgumentCaptor<Runnable> captor = ArgumentCaptor.forClass(Runnable.class);

        // Record the number of times publishAggregatedMarketData is called and the data published
        processor = new MarketDataProcessor(fakeTime, mockScheduler) {
            @Override
            public void publishAggregatedMarketData(MarketData data) {
                super.publishAggregatedMarketData(data);
                publishedEntries.add(data);
            }
        };

        // 2. Capture the scheduled task that MarketDataProcessor schedules in its constructor
        verify(mockScheduler).scheduleAtFixedRate(captor.capture(), anyLong(), anyLong(), any(TimeUnit.class));
        capturedTask = captor.getValue();
    }

    @Test
    // @CsvSource({"100,10,100"})
    void testRateLimiting() throws InterruptedException {

        // Arrange: Prepare the 100 symbol data and invoke onMessage ...
        final int msSleepTimeForEachSymbolRecord = 0;
        // Send a single update per symbol at T=0 (one record each)
        generateData(processor, symbols.stream().limit(100).collect(Collectors.toList()), msSleepTimeForEachSymbolRecord, 10);

        // Act: Execute the scheduler the first time ...
        capturedTask.run();

        // Assert: Ensure that only 100 entries are published (due to the 100 per second limit)
        assertEquals(100, publishedEntries.size());

        // Assert: Ensure no symbol is published more than once in the current window
        assertFalse(hasAnyDuplicateSymbols(), "publishedEntries should not contain any duplicate symbols");

        // Act: advance 500ms and send 10 updates within same window
        ((FakeTimeProvider) fakeTime).advance(500);
        List<String> firstTenSym = symbols.stream().limit(10).collect(Collectors.toList());
        generateData(processor, firstTenSym, 0, 1); // Send one more update for the first 10 symbols

        // Running the scheduler again and still the 100 publish quota limits anymore publish. The additional 10 updated symbols are not published yet ...
        capturedTask.run();
        assertEquals(100, publishedEntries.size(), "Second run within same window should publish 0 new items");

        // Act: advance beyond 1s window (another 501ms) and run the task -> the additional 10 updated symbols should be published
        ((FakeTimeProvider) fakeTime).advance(501);
        capturedTask.run();
        assertEquals(110, publishedEntries.size(), "Third run after window should publish the 10 updated symbols (total 110)");
    }

    @Test
    void test3180DistinctSymbols() {
        final int totalSymbols = 3180;

        // Generate 3180 unique symbols and feed them in at t=0
        for (int i = 0; i < totalSymbols; i++) {
            String sym = "S" + i;
            LocalDateTime updateTime = LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(fakeTime.currentTimeMillis()), java.time.ZoneId.systemDefault());
            processor.onMessage(MarketData.builder().symbol(sym).bid(1.0).ask(1.0).updateTime(updateTime).build());
        }

        // Run the scheduled publishing once at t=0 -> should publish up to 100
        capturedTask.run();
        assertEquals(100, publishedEntries.size(), "First run should publish exactly 100 entries");

        // Now repeatedly advance the fake time by just over 1s and run the task until all symbols are published
        int iterations = 0;
        while (publishedEntries.size() < totalSymbols) {
            iterations++;
            ((FakeTimeProvider) fakeTime).advance(1001); // advance beyond the 1s sliding window
            int before = publishedEntries.size();
            capturedTask.run();
            int after = publishedEntries.size();
            int delta = after - before;
            // Each second we must not publish more than 100 entries
            assertTrue(delta <= 100, "Per-second publish exceeded 100: " + delta);
            // If no progress is made and we still have items, fail to avoid infinite loop
            if (delta == 0) {
                fail("No progress made in iteration " + iterations + "; stuck before publishing all symbols");
            }
            // Safety - should complete in at most 1000 iterations
            if (iterations > 1000) {
                fail("Too many iterations: " + iterations);
            }
        }

        // All symbols are eventually published ...
        assertEquals(totalSymbols, publishedEntries.size(), "All symbols should eventually be published");

        // Number of iterations (seconds) required should equal ceil(totalSymbols / 100.0)
        int expectedSeconds = (int) Math.ceil(totalSymbols / 100.0);
        // We used iterations additional runs after the initial run; totalRuns = 1 + iterations
        assertEquals(expectedSeconds, 1 + iterations, "Total publishing runs (seconds) should match expected ceiling");
    }

    private void generateData(IMessageListener marketDataProcessor, List<String> symbols, int sleepTimeForEachSymbolRecord, int noOfRecords) throws InterruptedException {
        for (int i = 1; i <= noOfRecords; i++) {
            int finalI = i;
            for (String symbol : symbols) {
                // Use fakeTime for deterministic updateTime
                LocalDateTime updateTime = LocalDateTime.ofInstant(java.time.Instant.ofEpochMilli(fakeTime.currentTimeMillis()), java.time.ZoneId.systemDefault());
                marketDataProcessor.onMessage(MarketData.builder().symbol(symbol).bid(finalI).ask(finalI).updateTime(updateTime).build());
            }
            Thread.sleep(sleepTimeForEachSymbolRecord);
        }
    }

    private boolean hasAnyDuplicateSymbols() {
        return publishedEntries.stream()
                .map(MarketData::getSymbol)
                .distinct()
                .count() < publishedEntries.size();
    }

}
