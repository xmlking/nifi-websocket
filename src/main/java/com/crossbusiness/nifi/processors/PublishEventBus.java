package com.crossbusiness.nifi.processors;

import com.crossbusiness.nifi.controllers.VertxServiceInterface;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.json.JsonObject;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.stream.io.BufferedInputStream;
import org.apache.nifi.util.ObjectHolder;

import java.io.IOException;
import java.io.InputStream;
import java.util.*;

@EventDriven
@SeeAlso(classNames = {"GetEventBus", "SendEventBus", "VertxService"})
@Tags({"egress", "publish", "websocket", "ws", "wss",  "sockJS", "eventbus"})
@WritesAttribute(attribute = "eventbus.address", description = "The name of the eventbus's address to which the flowFile was published")
@CapabilityDescription("publish flowFile to eventbus. all subscribers on the address will receive flowFile")
public class PublishEventBus extends AbstractProcessor {

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Relationship for successfully received FlowFiles")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("any flowFile that cannot be published to EventBus will be routed to failure")
            .build();

    public static final PropertyDescriptor OUTBOUND_ADDRESS  = new PropertyDescriptor.Builder()
            .name("outbound address")
            .description("address where outbound message will be published")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor VERTX_SERVICE = new PropertyDescriptor.Builder()
            .name("Vertx Service").description("The ControllerService that is used to obtain eventBus instance")
            .identifiesControllerService(VertxServiceInterface.class).required(true).build();

    protected volatile EventBus eventBus;

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    protected void init(final ProcessorInitializationContext context) {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        this.relationships = Collections.unmodifiableSet(relationships);

        final List<PropertyDescriptor> properties = new ArrayList<PropertyDescriptor>();
        properties.add(OUTBOUND_ADDRESS);
        properties.add(VERTX_SERVICE);
        this.properties = Collections.unmodifiableList(properties);
    }

    @OnScheduled
    public void setup(final ProcessContext context) {
        final VertxServiceInterface vertxService = context.getProperty(VERTX_SERVICE).asControllerService(VertxServiceInterface.class);
        eventBus = vertxService.getEventBus();
    }

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

            eventBus.publish(outboundAddress,new JsonObject(rootNode.toString()));

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
