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
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import javax.batch.operations.JobStartException;
import java.util.Date;

@Component
public class JobRunner {

    public static final Logger logger = LoggerFactory.getLogger(JobRunner.class);

    private final Job databaseToCSVFileJob;
    @Value("${database.to.csv.job.export.file.path}")
    private String outputSourceFile;

    private final JobLauncher simpleJobLauncher;
    private final Job csvFileToDatabaseJob;
    @Value("${csv.to.database.job.source.file.path}")
    private String inputSourceFile;

    @Autowired
    public JobRunner(JobLauncher simpleJobLauncher,
                     @Qualifier("csvFileToDatabase") Job csvFileToDatabaseJob,
                     @Qualifier("databaseToCsvFile") Job databaseToCSVFileJob) {
        this.simpleJobLauncher = simpleJobLauncher;
        this.csvFileToDatabaseJob = csvFileToDatabaseJob;
        this.databaseToCSVFileJob = databaseToCSVFileJob;
    }

    @Async
    public void runCSVFileToDatabaseBatchJob() {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(Constants.FILE_NAME_CONTEXT_KEY, inputSourceFile);
        jobParametersBuilder.addDate("date", new Date(), true);
        runCSVFileToDatabaseJob(csvFileToDatabaseJob, jobParametersBuilder.toJobParameters());
    }

    public void runCSVFileToDatabaseJob(Job job, JobParameters jobParameters) {
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

    @Async
    public void runDatabaseToCSVFileBatchJob() {
        JobParametersBuilder jobParametersBuilder = new JobParametersBuilder();
        jobParametersBuilder.addString(Constants.OUTPUT_FILE_NAME_CONTEXT_KEY, outputSourceFile);
        jobParametersBuilder.addDate("date", new Date(), true);
        runDatabaseToCSVFileJob(databaseToCSVFileJob, jobParametersBuilder.toJobParameters());
    }

    public void runDatabaseToCSVFileJob(Job job, JobParameters jobParameters) {
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
