package com.temporal.demos.temporalspringbootdemo.workflows;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.temporal.demos.temporalspringbootdemo.activities.DemoActivities;
import com.temporal.demos.temporalspringbootdemo.service.PythonClient;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import io.temporal.activity.ActivityOptions;
import io.temporal.spring.boot.WorkflowImpl;
import io.temporal.workflow.Workflow;
import org.springframework.beans.factory.annotation.Autowired;

import java.net.URI;
import java.nio.charset.Charset;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

@WorkflowImpl(taskQueues = "DemoTaskQueue")
public class DemoWorkflowImpl implements DemoWorkflow {

    private List<CloudEvent> eventList = new ArrayList<>();

    private DemoActivities demoActivities =
            Workflow.newActivityStub(DemoActivities.class,
                    ActivityOptions.newBuilder()
                            .setStartToCloseTimeout(Duration.ofSeconds(2))
                            .build());

    private final PythonClient pythonClient;

    @Autowired
    public DemoWorkflowImpl(PythonClient pythonClient) {
        this.pythonClient = pythonClient;
    }

    @Override // WorkflowMethod
    public CloudEvent exec(CloudEvent cloudEvent) {
        eventList.add(cloudEvent);

        demoActivities.before(cloudEvent);

        // wait for second event

        Workflow.await(() -> eventList.size() == 2);

        demoActivities.after(cloudEvent);

        // return demo result CE
        ObjectMapper mapper = new ObjectMapper();
        JsonNode node = mapper.createObjectNode();
        ((ObjectNode)node).putArray("events");
        for(CloudEvent c : eventList) {
            ((ArrayNode)node.get("events")).add(c.getId());
        }
        ((ObjectNode)node).put("outcome", "done");
        return CloudEventBuilder.v1()
                .withId(String.valueOf(Workflow.newRandom().nextInt(1000 - 1 + 1) + 1))
                .withType("example.demo.result")
                .withSource(URI.create("http://temporal.io"))
                .withData(
                        "application/json",
                        (node.toPrettyString())
                                .getBytes(Charset.defaultCharset()))
                .build();

    }

    @Override // SignalMethod
    public void addEvent(CloudEvent cloudEvent) {
        eventList.add(cloudEvent);
    }

    @Override // QueryMethod
    public CloudEvent getLastEvent() {
        return eventList.get(eventList.size() - 1);
    }

    // Method calling python activities.
    public String callPythonActivity(String name) {
        return pythonClient.greetInSpanish(name);
    }
}
