package com.crossbusiness.nifi.processors;

import com.crossbusiness.nifi.controllers.VertxService;
import io.vertx.core.eventbus.EventBus;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.util.*;
import java.util.concurrent.TimeUnit;

@EventDriven
@Tags({"egress", "put", "websocket", "eventbus", "ws", "wss"})
@WritesAttributes({
        @WritesAttribute(attribute = "eventbus.topic", description = "The name of the eventbus Topic from which the message was received")
})
@CapabilityDescription("sent FlowFile to eventbus")

public class PutEventBus extends AbstractProcessor {

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Relationship for successfully received FlowFiles")
            .build();

    public static final Relationship REL_FAILURE = new Relationship.Builder()
            .name("failure")
            .description("Any FlowFile that cannot be sent to EventBus will be routed to failure")
            .build();

    public static final PropertyDescriptor OUTBOUND_ADDRESS  = new PropertyDescriptor.Builder()
            .name("outbound address")
            .description("permitted matches for outbound traffic")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor VERTX_SERVICE = new PropertyDescriptor.Builder()
            .name("Vertx Service").description("The Controller Service that is used to obtain vert.x and eventBus instances")
            .identifiesControllerService(VertxService.class).required(true).build();

    @Override
    protected void init(final ProcessorInitializationContext context) {
        //final VertxService vertxService = (VertxService) context.getControllerServiceLookup().getControllerService("vertx");
    }

    @Override
    public Set<Relationship> getRelationships() {
        //return Collections.singleton(REL_SUCCESS);
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return Collections.unmodifiableSet(relationships);
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<PropertyDescriptor>();
        properties.add(OUTBOUND_ADDRESS);
        properties.add(VERTX_SERVICE);
        return Collections.unmodifiableList(properties);
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile incoming = session.get();
        if ( incoming == null ) {
            return;
        }

        final ProcessorLog logger = getLogger();

        final String outboundAddress = context.getProperty(OUTBOUND_ADDRESS).getValue();
        final VertxService vertxService = context.getProperty(VERTX_SERVICE).asControllerService(VertxService.class);

        final EventBus eb = vertxService.getEventBus();

        try {
            final StopWatch stopWatch = new StopWatch(true);

            eb.send(outboundAddress,incoming);



            FlowFile outgoing = session.clone(incoming); //session.create();
            final Map<String, String> attributes = new HashMap<>();
            attributes.put("eventbus.topic", outboundAddress);
            outgoing = session.putAllAttributes(outgoing, attributes);

            // TODO implement
            logger.info("Successfully {} {}",new Object[]{"sent", incoming});
            session.getProvenanceReporter().modifyContent(outgoing, stopWatch.getElapsed(TimeUnit.MILLISECONDS));
            session.transfer(outgoing, REL_SUCCESS);
        } catch (ProcessException e) {
            logger.error("Failed to {} {}",new Object[]{"sent", incoming});
            session.transfer(incoming, REL_FAILURE);
        }
    }
}
