package com.mkt.marketdata;

public interface IMessageListener {
    /**
     * Callback function for receiving incoming market data feed
     *
     * @param data Market data {@link MarketData}
     */
    void onMessage(MarketData data);
}
