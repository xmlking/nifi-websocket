package com.crossbusiness.nifi.processors;

import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;

@EventDriven
@SeeAlso(classNames = {"PublishEventBus", "GetEventBus", "VertxService"})
@Tags({"egress", "send", "websocket", "ws", "wss",  "sockJS", "eventbus"})
@WritesAttribute(attribute = "eventbus.address", description = "The name of the eventbus's address to which the flowFile was sent")
@CapabilityDescription("sent flowFile to eventBus (point-to-point). only one recipient will receive flowFile")
public class SendEventBus extends PublishEventBus {

    @Override
    protected void put(final String address, final Object message) {
        eventBus.send(address,message);
    }
}
