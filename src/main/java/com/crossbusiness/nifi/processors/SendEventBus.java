package com.crossbusiness.nifi.processors;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.json.JsonObject;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.util.ObjectHolder;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;

@EventDriven
@SeeAlso(classNames = {"PublishEventBus", "GetEventBus", "VertxService"})
@Tags({"egress", "send", "websocket", "ws", "wss",  "sockJS", "eventbus"})
@WritesAttribute(attribute = "eventbus.address", description = "The name of the eventbus's address to which the flowFile was sent")
@CapabilityDescription("sent flowFile to eventbus. only one recipient will receive flowFile")
public class SendEventBus extends PublishEventBus {

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile incoming = session.get();
        if ( incoming == null ) {
            return;
        }

        final String outboundAddress = context.getProperty(OUTBOUND_ADDRESS).getValue();

        try {
            //final StopWatch stopWatch = new StopWatch(true);

            // Parse the JSON document
            final ObjectMapper mapper = new ObjectMapper();
            final ObjectHolder<JsonNode> rootNodeRef = new ObjectHolder<>(null);
            session.read(incoming, new InputStreamCallback() {
                @Override
                public void process(final InputStream in) throws IOException {
                    try (final InputStream bufferedIn = new BufferedInputStream(in)) {
                        rootNodeRef.set(mapper.readTree(bufferedIn));
                    }
                }
            });
            final JsonNode rootNode = rootNodeRef.get();

            eventBus.send(outboundAddress,new JsonObject(rootNode.toString()));

            //FlowFile outgoing = session.clone(incoming);
            final Map<String, String> attributes = new HashMap<>();
            attributes.put("eventbus.address", outboundAddress);
            incoming = session.putAllAttributes(incoming, attributes);
            //session.getProvenanceReporter().modifyContent(incoming, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(incoming, REL_SUCCESS);
        } catch (ProcessException pe) {
            getLogger().error("Failed to parse {} as JSON due to {}; routing to failure", new Object[] {incoming, pe.toString()}, pe);
            session.transfer(incoming, REL_FAILURE);
        }
    }
}
