# NiFi-WebSocket

```python
Work-in-Progress
```
The goal of this project is to enable *WebSocket* interface to NiFi so that, external clients can send data to a flow as well as NiFi can stream results back to clients like browsers.
To simplify usage, [STOMP over WebSocket](http://jmesnil.net/stomp-websocket/doc/) protocol is used for client/server communication.   
   
1. **VertxService** is a NiFi service that opens **WebSocket** port and bridge the embedded [vertx's](http://vertx.io/) **EventBus**.  
2. **GetEventBus** is a NiFi processor that subscribe to **EventBus's** address. all messages received on the address will be emitted into the flow.  
3. **SendEventBus** is a NiFi processor that sends flowFile to **EventBus** address (point-to-point). only one recipient will receive flowFile.  
4. **PublishEventBus** is a NiFi processor that publish flowFile to **EventBus** address (pub/sub). all subscribers on the address will receive flowFile. 

### Install
1. Manual: Download [Apache NiFi](https://nifi.apache.org/download.html) binaries and unpack to a folder. 
2. On Mac: brew install nifi

### Deploy
```bash
# Assume you unpacked nifi-0.3.0-bin.zip to /Developer/Applications/nifi
./gradlew clean deploy -Pnifi_home=/Developer/Applications/nifi
```
On Mac 
```bash
gradle clean deploy -Pnifi_home=/usr/local/Cellar/nifi/0.3.0/libexec
```

### Run
```bash
cd /Developer/Applications/nifi
./bin/nifi.sh  start
./bin/nifi.sh  stop
```
On Mac 
```bash
# nifi start|stop|run|restart|status|dump|install
nifi start 
nifi status  
nifi stop 
# Working Directory: /usr/local/Cellar/nifi/0.3.0/libexec
```
### Test

1. check if SockJS server is up: http://hostname:{port}/{eventbus}/info
2. test evenBus via web page: [test.html](./test.html)
3. test with flow: [WebSocketFlow.xml](./WebSocketFlow.xml)

### TODO
1. Try HazelcastClusterManager and Vertx.clusteredVertx to see if vertx clustering is possible with NiFi cluster. 
2. Support ability to publish multiple types of messages. i.e., primitives, string, buffers, JSON