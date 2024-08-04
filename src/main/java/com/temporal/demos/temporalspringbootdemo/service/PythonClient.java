package com.temporal.demos.temporalspringbootdemo.service;

import io.temporal.activity.ActivityOptions;
import io.temporal.workflow.ActivityStub;
import io.temporal.workflow.Workflow;
import org.springframework.stereotype.Service;

import java.time.Duration;

@Service
public class PythonClient {

    private final ActivityOptions spanishGreetingActivityOptions
            = ActivityOptions.newBuilder()
            .setStartToCloseTimeout(Duration.ofSeconds(10))
            // todo move to an enum in proto
            .setTaskQueue("python-greeting-tasks")
            .build();

    private final ActivityStub spanishGreetingActivityStub;

    public PythonClient() {
        this.spanishGreetingActivityStub =  Workflow.newUntypedActivityStub(spanishGreetingActivityOptions);
    }

    // Note: testing activity in java for data-convertor
    public String greetInSpanish(String name) {
        return spanishGreetingActivityStub.execute("JavaSpanishGreetingActivity", String.class, name);
    }
}
