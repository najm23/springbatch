package com.starapp.springbatch.controller;

import com.starapp.springbatch.runner.JobRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/run")
public class JobController {

    private final JobRunner jobRunner;

    @Autowired
    public JobController(JobRunner jobRunner) {
        this.jobRunner = jobRunner;
    }

    @RequestMapping("/job")
    public String runJob() {
        jobRunner.runBatchJob();
        return "Job demo1 submitted successfully.";
    }
}
