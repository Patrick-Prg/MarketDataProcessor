package com.mkt.marketdata;

/**
 * Abstraction for time provision, allowing injection of real or fake time for testing
 */
public interface TimeProvider {
    /**
     * Get the current time in milliseconds
     * @return current time in milliseconds
     */
    long currentTimeMillis();
}

