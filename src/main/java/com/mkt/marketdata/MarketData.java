package com.mkt.marketdata;

import lombok.Builder;
import lombok.Data;

import java.time.LocalDateTime;

/**
 * Market Data Message Class
 */
@Data
@Builder
public class MarketData {
    private String symbol;
    private double bid;
    private double ask;
    private double last;
    private LocalDateTime updateTime;
}
