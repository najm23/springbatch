package com.starapp.springbatch.job;

import com.starapp.springbatch.dto.EmployeeDTO;
import com.starapp.springbatch.mapper.EmployeeFileRawMapper;
import com.starapp.springbatch.model.Employee;
import com.starapp.springbatch.processor.EmployeeCSVtoDatabaseProcessor;
import com.starapp.springbatch.writer.EmployeeDBWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecution;
import org.springframework.batch.core.JobExecutionListener;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import java.util.concurrent.TimeUnit;

@Configuration
public class CsvFileToDatabaseWithTaskExecutor {

    private static final Logger logger = LoggerFactory.getLogger(CsvFileToDatabaseWithTaskExecutor.class);


    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EmployeeCSVtoDatabaseProcessor employeeProcessor;
    private final EmployeeDBWriter employeeDBWriter;

    @Autowired
    public CsvFileToDatabaseWithTaskExecutor(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory,
                                             EmployeeCSVtoDatabaseProcessor employeeProcessor, EmployeeDBWriter employeeDBWriter) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.employeeProcessor = employeeProcessor;
        this.employeeDBWriter = employeeDBWriter;
    }

    @Qualifier("csvFileToDatabaseWithTaskExecutor")
    @Bean
    public Job csvFileToDatabaseWithTaskExecutorJob() {
        return this.jobBuilderFactory.get("csvFileToDatabaseWithTaskExecutor")
                .start(csvFileToDatabaseWithTaskExecutorStep())
                .listener(new JobExecutionListener() {
                    @Override
                    public void beforeJob(JobExecution jobExecution) {

                    }

                    @Override
                    public void afterJob(JobExecution jobExecution) {
                        // get diff between end time and start time
                        long diff = jobExecution.getEndTime().getTime() - jobExecution.getCreateTime().getTime();

                        String duration = String.format("%d h %d min %d sec", TimeUnit.MILLISECONDS.toHours(diff),
                                TimeUnit.MILLISECONDS.toMinutes(diff) - TimeUnit.HOURS.toMinutes(TimeUnit.MILLISECONDS.toHours(diff)),
                                TimeUnit.MILLISECONDS.toSeconds(diff) - TimeUnit.MINUTES.toSeconds(TimeUnit.MILLISECONDS.toMinutes(diff)));
                        // log  time
                        logger.info("Step execution time = {} ", duration);
                    }
                })
                .build();
    }

    @Bean
    public Step csvFileToDatabaseWithTaskExecutorStep() {
        return this.stepBuilderFactory.get("CsvFileToDatabaseWithTaskExecutorStep")
                .<EmployeeDTO, Employee>chunk(10)
                .reader(employeeDataReader())
                .processor(employeeProcessor)
                .writer(employeeDBWriter)
                .taskExecutor(taskExecutor())
                .build();
    }

    @Bean
    @StepScope
    Resource inputDataFileResource(@Value("#{jobParameters[fileName]}") final String fileName) {
        if (fileName == null)
            throw new IllegalArgumentException("The object 'fileName' cannot be null");
        return new ClassPathResource(fileName);
    }

    @Bean
    @StepScope
    public FlatFileItemReader<EmployeeDTO> employeeDataReader() {
        FlatFileItemReader<EmployeeDTO> reader = new FlatFileItemReader<>();
        reader.setResource(inputDataFileResource(""));
        reader.setLineMapper(new DefaultLineMapper<>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames("employeeId", "firstName", "lastName", "email", "age");
            }});
            setFieldSetMapper(new EmployeeFileRawMapper());
        }});
        return reader;
    }

    @Bean
    public TaskExecutor taskExecutor() {
        SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor();
        simpleAsyncTaskExecutor.setConcurrencyLimit(5);
        return simpleAsyncTaskExecutor;
    }

}
