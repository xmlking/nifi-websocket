# nifi-websocket

```python
Work-in-Progress
```

**ListenSockJS** is a NiFi processor that opens websocket port and ingress data into flow.  
**PutEventBus** is a NiFi processor that egress data into vert.x **EventBus** from flow. 

### Deploy
```
./gradlew clean deploy -Pnifi_home=/Developer/Applications/nifi
```

### Run
```bash
cd /Developer/Applications/nifi
./bin/nifi.sh  start
./bin/nifi.sh  stop
```