# trading-singleleg
This contains set of Java programs to automatically manage (exits) trades based on configurable rules using IB API. 
Programs expects entry signals to be pushed to Redis queue for entry of trades.

## Features

  * Performs trade entry based on entry signals arriving in the defined queue of Redis
  * Supports market orders and relative orders
  * Once trades are entered, keeps monitoring and take actions based on parameters/configurations, stop loss limit, take profit limits, holding period etc.
  * Exits the trades based on pre-set rules.
  * Provision to perform manual interventions (currently supports square off, adjustment of take profit limits and adjustment of stop loss limits)
  * ...
  * ...

## Instructions

Requires following :
  * IB TWS to be running and accessible from the machine where this Java program is running.
  * Redis server to be running and accessible from the machine where this Java programs is running.
  * Predefined key structure to store configuration is available on Redis server. 'redis-configuration.txt' contains required keys.

### Running the programs

Please download all the files, and compile to make jar file. 
Run the jar file as follows :

	Usage : <fileName.jar> <debugFlag> <redisServerIPAddress> <redisServerPortNumber> <redisServerConfigKey> <TWSIPAddress> <TWSPortNumber> <TWSClientID> <ExchangeCurrency>

    debugFlag is debug flag. 1 is true. 0 is false
    redisServerIPAddress is ip address/machine name where Redis Server is running
    redisServerPortNumber is port number on which Redis Server is listening for connections        
    redisServerConfigKey is configKey in Redis Server to use.'redis-configuration.txt' contains required keys.  
    TWSIPAddress is ip address/machine name where TWS is running
    TWSPortNumber is port number on which TWS is listening for API calls
    TWSClientID is client id to be used for trading through IB TWS
    ExchangeCurrency is exchange currency to be used for this session. based on that timezone gets set. supported values are "inr" or "usd"

## Troubleshooting

If you have problems using the software, please try to see if all pre-requisite / requirements are taken care.

## Filing an issue/Reporting a bug

When reporting bugs against `trading-singleleg`, please don't forget to include enough information :

* Is the problem happening with the latest version ?
* What operating system are you using ?
* Do you have all the recommended versions of the modules? See them in
  the file `requirements.txt`.
* What is the precise message you are getting or unexpected behavior you are experiencing ? Please, copy and paste them.
  Don't reword the messages.
  
## Feedback

I enjoy getting feedback. Please do send feedback to contacts below.

## Contact

Post bugs and issues on [github]. 
Send other comments to Manish Singh: mksingh at hotmail dotcom
