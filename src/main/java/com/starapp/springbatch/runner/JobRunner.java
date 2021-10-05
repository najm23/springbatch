package com.starapp.springbatch.runner;

import com.starapp.springbatch.utils.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.*;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.batch.core.repository.JobExecutionAlreadyRunningException;
import org.springframework.batch.core.repository.JobInstanceAlreadyCompleteException;
import org.springframework.batch.core.repository.JobRestartException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.batch.operations.JobStartException;
import java.util.Date;

@Component
public class JobRunner {

    public static final Logger logger = LoggerFactory.getLogger(JobRunner.class);

    private final JobLauncher simpleJobLauncher;
    private final Job demo1;

    @Autowired
    public JobRunner(JobLauncher simpleJobLauncher, Job job) {
        this.simpleJobLauncher = simpleJobLauncher;
        this.demo1 = job;
    }

    @Async
    public void runBatchJob() {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(Constants.FILE_NAME_CONTEXT_KEY, "employees.csv");
        jobParametersBuilder.addDate("date", new Date(), true);
        runJob(demo1, jobParametersBuilder.toJobParameters());
    }

    public void runJob(Job job, JobParameters jobParameters) {
        try {
            JobExecution jobExecution = simpleJobLauncher.run(job, jobParameters);
        } catch (JobExecutionAlreadyRunningException e) {
            logger.info("Job with fileName={} is already running.", jobParameters.getParameters().get(Constants.FILE_NAME_CONTEXT_KEY));
        } catch (JobStartException e) {
            logger.info("Job with fileName={} was not started.", jobParameters.getParameters().get(Constants.FILE_NAME_CONTEXT_KEY));
        } catch (JobInstanceAlreadyCompleteException e) {
            logger.info("Job with fileName={} is already completed.", jobParameters.getParameters().get(Constants.FILE_NAME_CONTEXT_KEY));
        } catch (JobParametersInvalidException e) {
            logger.info("Invalid parameters for job with fileName={} .", jobParameters.getParameters().get(Constants.FILE_NAME_CONTEXT_KEY));
        } catch (JobRestartException e) {
            e.printStackTrace();
        }
    }
}
