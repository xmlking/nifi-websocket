# nifi-websocket

```python
Work-in-Progress
```

**ListenSockJS** is a NiFi processor that opens websocket port and ingress data into flow.  
**PutEventBus** is a NiFi processor that egress data into vert.x **EventBus** from flow. 

### Install
Download [Apache NiFi](https://nifi.apache.org/download.html) binaries and unpack to a folder. 

### Deploy
```
# Assume you unpacked nifi-0.3.0-bin.zip to /Developer/Applications/nifi
./gradlew clean deploy -Pnifi_home=/Developer/Applications/nifi
# gradle clean deploy -Pnifi_home=/Developer/Applications/nifi
```

### Run
```bash
cd /Developer/Applications/nifi
./bin/nifi.sh  start
./bin/nifi.sh  stop
```