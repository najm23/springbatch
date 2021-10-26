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

    @RequestMapping("/csvToDbJob")
    public String runCSVFileToDatabaseJob() {
        jobRunner.runCSVFileToDatabaseBatchJob();
        return "Job csvFileToDatabaseJob submitted successfully.";
    }

    @RequestMapping("/dbToCsvJob")
    public String runDatabaseToCSVFileJob() {
        jobRunner.runDatabaseToCSVFileBatchJob();
        return "Job databaseToCSVFileJob submitted successfully.";
    }

    @RequestMapping("/csvToDbTaskExecutorJob")
    public String runCSVFileToDatabaseTaskExecutorJob() {
        jobRunner.runCsvFileToDatabaseWithTaskExecutorBatchJob();
        return "Job csvFileToDatabaseWithTaskExecutorJob submitted successfully.";
    }

    @RequestMapping("/csvToDbMultiStepsJob")
    public String runCSVFileToDatabaseMultiStepsJob() {
        jobRunner.runCSVFileToDatabaseMultiStepsJob();
        return "Job runCSVFileToDatabaseMultiStepsJob submitted successfully.";
    }
}
