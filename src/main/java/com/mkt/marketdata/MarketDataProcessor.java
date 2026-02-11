package com.mkt.marketdata;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.ArrayDeque;
import java.util.Deque;

/**
 * A class that handling market data feed
 *
 * Implements sliding-window rate limiting (max 100 publications per second)
 * and ensures each symbol is published at most once per sliding window.
 */
public class MarketDataProcessor implements IMessageListener {

    // Sliding window of 1 second (1000ms)
    private static final long WINDOW_SIZE_MS = 1000L;
    private static final int MAX_PUBLISHES_PER_WINDOW = 100;

    // 將所有狀態封裝，減少 Map 查詢次數
    private static class SymbolState {
        MarketData data;
        long lastDataMs = -1;
        long lastPublishMs = -1;
    }

    private final ConcurrentHashMap<String, SymbolState> states = new ConcurrentHashMap<>();
    private final Deque<Long> windowTimestamps = new ArrayDeque<>();
    private final TimeProvider timeProvider;
    private final ScheduledExecutorService scheduler;

    public MarketDataProcessor(TimeProvider timeProvider, ScheduledExecutorService scheduler) {
        this.timeProvider = timeProvider;
        this.scheduler = scheduler;
        this.scheduler.scheduleAtFixedRate(this::tryPublishNextBatch, 10, 10, TimeUnit.MILLISECONDS);
    }

    @Override
    public void onMessage(MarketData data) {
        // 在進入點處理好時間轉換，避免在循環中重複處理
        states.computeIfAbsent(data.getSymbol(), k -> new SymbolState()).data = data;
    }

    private synchronized void tryPublishNextBatch() { // 使用 synchronized 保護 windowTimestamps
        long now = timeProvider.currentTimeMillis();
        long windowStart = now - WINDOW_SIZE_MS;

        // 1. Clear outdated timestamps from the sliding window
        while (!windowTimestamps.isEmpty() && windowTimestamps.peekFirst() < windowStart) {
            windowTimestamps.removeFirst();
        }

        int quota = MAX_PUBLISHES_PER_WINDOW - windowTimestamps.size();
        if (quota <= 0) return;

        // Read through the symbols and publish if they have new data
        // and haven't been published in the current window
        for (SymbolState state : states.values()) {
            if (quota <= 0) break;

            MarketData data = state.data;
            if (data == null) continue;

            long dataMs = toEpochMilli(data.getUpdateTime());

            // Conditions:
            // 1. Make sure it is new data and
            // 2. Either not published before or last publish is outside the current window ...
            if (dataMs > state.lastDataMs && (state.lastPublishMs < windowStart || state.lastPublishMs < 0)) {
                publishAggregatedMarketData(data);

                state.lastDataMs = dataMs;
                state.lastPublishMs = now;
                windowTimestamps.addLast(now);
                quota--;
            }
        }
    }

    private long toEpochMilli(java.time.LocalDateTime ldt) {
        return ldt == null ? 0L : ldt.atZone(java.time.ZoneId.systemDefault()).toInstant().toEpochMilli();
    }

    /**
     * Publish aggregated and throttled market data
     *
     * @param data Market data {@link MarketData}
     */
    public void publishAggregatedMarketData(MarketData data) {
        // Do nothing, assume implemented.
    }
}
