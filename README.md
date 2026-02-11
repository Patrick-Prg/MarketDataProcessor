# Market Data Processor


## Versions

- Java version 17/+
-  Gradle version 9.2.0

## Requirements

- Ensure that the publishAggregatedMarketData method is not called any more than 100 times/sec where this period is a
  sliding window.
- Ensure each symbol will not have more than one update per second
- Ensure each symbol will always have the latest market data when it is published

## Assumptions

- All calls to the onMessage method are made by one thread.
- The MarketDataProcessor receives messages from some source via the onMessage call back method. The incoming rate is
  unknown.
- The computer memory running the program is large enough to cache at least one market data for all the symbols.
- "latest market data" means the data I published were the most recent market data I received at that moment.

## How to run the test cases

- execute "gradlew test"

