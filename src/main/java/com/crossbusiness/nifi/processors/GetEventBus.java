package com.crossbusiness.nifi.processors;

import com.crossbusiness.nifi.controllers.VertxServiceInterface;
import io.vertx.core.MultiMap;
import io.vertx.core.eventbus.EventBus;
import io.vertx.core.eventbus.Message;
import io.vertx.core.eventbus.MessageConsumer;
import io.vertx.core.json.JsonObject;
import org.apache.nifi.annotation.behavior.EventDriven;
import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.SeeAlso;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.annotation.lifecycle.OnStopped;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.flowfile.attributes.CoreAttributes;
import org.apache.nifi.logging.ProcessorLog;
import org.apache.nifi.processor.*;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.util.StopWatch;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

@EventDriven
@SeeAlso(classNames = {"PublishEventBus", "SendEventBus", "VertxService"})
@Tags({"ingest", "get", "websocket", "ws", "wss",  "sockJS", "eventbus", "listen"})
@WritesAttribute(attribute = "eventbus.address", description = "The name of the eventbus's address from which the message was received")
@CapabilityDescription("subscribe to eventbus's address. all messages received on the address will be emitted into the flow")
public class GetEventBus extends AbstractProcessor {

    private Set<Relationship> relationships;
    private List<PropertyDescriptor> properties;

    public static final Relationship REL_SUCCESS = new Relationship.Builder()
            .name("success")
            .description("Relationship for successfully received FlowFiles")
            .build();

    public static final PropertyDescriptor INBOUND_ADDRESS = new PropertyDescriptor.Builder()
            .name("inbound address")
            .description("The address where this processor will subscribe to eventBus to receive messages")
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor HEADERS_AS_ATTRIBUTES_REGEX = new PropertyDescriptor.Builder()
            .name("Message Headers to receive as Attributes (Regex)")
            .description("Specifies the Regular Expression that determines the names of Message Headers that should be passed along as FlowFile attributes")
            .addValidator(StandardValidators.REGULAR_EXPRESSION_VALIDATOR)
            .required(false)
            .build();

    public static final PropertyDescriptor VERTX_SERVICE = new PropertyDescriptor.Builder()
            .name("Vertx Service").description("The ControllerService that is used to obtain eventBus instance")
            .identifiesControllerService(VertxServiceInterface.class).required(true).build();

    private volatile MessageConsumer<JsonObject> consumer;
    private volatile BlockingQueue<Message<JsonObject>> messageQueue = new LinkedBlockingQueue<>(1000);
    private volatile Pattern headerPattern;

    @Override
    protected void init(final ProcessorInitializationContext context) {
        this.relationships = Collections.singleton(REL_SUCCESS);

        final List<PropertyDescriptor> properties = new ArrayList<PropertyDescriptor>();
        properties.add(INBOUND_ADDRESS);
        properties.add(HEADERS_AS_ATTRIBUTES_REGEX);
        properties.add(VERTX_SERVICE);
        this.properties = Collections.unmodifiableList(properties);
    }

    @Override
    public Set<Relationship> getRelationships() {
        return relationships;
    }

    @Override
    public final List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return properties;
    }

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        // if any property is modified, the results are no longer valid. Destroy all messages in the queue.
        messageQueue.clear();
    }

    @OnScheduled
    public void setup(final ProcessContext context) throws Exception {
        final VertxServiceInterface vertxService = context.getProperty(VERTX_SERVICE).asControllerService(VertxServiceInterface.class);
        final String inboundAddress = context.getProperty(INBOUND_ADDRESS).getValue();
        if (context.getProperty(HEADERS_AS_ATTRIBUTES_REGEX).isSet()) {
            headerPattern = Pattern.compile(context.getProperty(HEADERS_AS_ATTRIBUTES_REGEX).getValue());
        }
        final EventBus eventBus = vertxService.getEventBus();
        consumer = eventBus.consumer(inboundAddress);
        consumer.handler(message -> {
            getLogger().debug("I have received a message: " + message.body().toString());
            messageQueue.add(message);
        });
    }

    @OnStopped
    public void unregister() {
        if (consumer != null) {
            consumer.unregister(res -> {
                if (res.succeeded()) {
                    getLogger().info("The GetEventBus({}) handler un-registration has reached all nodes",new Object[] {consumer.address()});
                } else {
                    getLogger().info("GetEventBus({}) un-registration failed!",new Object[] {consumer.address()});
                }
            });
        }
    }

    @Override
    public void onTrigger(final ProcessContext context, final ProcessSession session) throws ProcessException {
        final ProcessorLog logger = getLogger();
        final Message<JsonObject> message = messageQueue.poll();
        if (message == null) {
            context.yield();
            return;
        }

        FlowFile flowFile = session.create();
        try {
            final StopWatch stopWatch = new StopWatch(true);

            flowFile = session.write(flowFile, new OutputStreamCallback() {
                @Override
                public void process(final OutputStream out) throws IOException {
                    out.write(message.body().encode().getBytes(StandardCharsets.UTF_8));
                }
            });

            if (flowFile.getSize() == 0L) {
                session.remove(flowFile);
            } else {
                final Map<String, String> attributes = new HashMap<>();

                if (headerPattern != null) {
                    MultiMap headers = message.headers();
                    headers.names()
                            .stream()
                            .filter(headerName -> headerPattern.matcher(headerName).matches())
                            .forEach(headerName -> attributes.put(headerName, headers.get(headerName)));

//                    attributes.putAll(
//                        headers.entries()
//                                .stream()
//                                .filter(entry -> headerPattern.matcher(entry.getKey()).matches())
//                                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue))
//                    );
                }
                attributes.put(CoreAttributes.MIME_TYPE.key(), "application/json");
                attributes.put(CoreAttributes.FILENAME.key(), flowFile.getAttribute(CoreAttributes.FILENAME.key()) + ".json");
                attributes.put("eventbus.address", message.address());

                flowFile = session.putAllAttributes(flowFile, attributes);
                session.getProvenanceReporter().receive(flowFile, message.address(), "received eventBus message", stopWatch.getElapsed(TimeUnit.MILLISECONDS));
                getLogger().info("Successfully received {} ({}) from EventBus in {} millis", new Object[]{flowFile, flowFile.getSize(), stopWatch.getElapsed(TimeUnit.MILLISECONDS)});
                session.transfer(flowFile, REL_SUCCESS);
            }
        } catch (Exception e) {
            session.remove(flowFile);
            throw e;
        }
    }
}


