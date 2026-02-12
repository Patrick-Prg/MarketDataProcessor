# Market Data Processor



## Versions

* Java version 17/+
* Gradle version 9.2.0

## Requirements

* Ensure that the number of calls of publishAggregatedMarketData method for publishing messages does not exceed 100 times per second, where this period is a sliding window.
* Ensure that each symbol does not update more than once per sliding window.
* Ensure that each symbol always has the latest market data published.
* Ensure the latest market data on each symbol will be published.



## Assumptions

* All calls to the onMessage method are made by one thread.
* The MarketDataProcessor receives messages from some source via the onMessage call back method. The incoming rate is
  unknown.
* The computer memory running the program is large enough to cache at least one market data for all the symbols.
* "latest market data" means the data I published were the most recent market data I received at that moment.

## 

## How to run the test cases

* execute "gradlew test"
