# nifi-websocket

```python
Work-in-Progress
```
**VertxService** is a NiFi service that opens **WebSocket** port and bridge **EventBus**.  
**GetEventBus** is a NiFi processor that subscribe to **EventBus's** address. all messages received on the address will be emitted into the flow.  
**SendEventBus** is a NiFi processor that publish flowFile to **EventBus**. all subscribers on the address will receive flowFile. 
**PublishEventBus** is a NiFi processor that sent flowFile to **EventBus**. only one recipient will receive flowFile. 

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

1. check if SockJS server is up: http://hostname:port/eventbus/info
2. test evenBus via web page: [test.html](./test.html)
3. test with flow: [WebSocketFlow](./WebSocketFlow.xml)