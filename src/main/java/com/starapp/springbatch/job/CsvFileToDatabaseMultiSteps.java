package com.starapp.springbatch.job;

import com.starapp.springbatch.dto.EmployeeDTO;
import com.starapp.springbatch.mapper.EmployeeDbRawMapper;
import com.starapp.springbatch.mapper.EmployeeFileRawMapper;
import com.starapp.springbatch.model.Employee;
import com.starapp.springbatch.processor.EmployeeCSVtoDatabaseProcessor;
import com.starapp.springbatch.writer.EmailSenderWriter;
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
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.mapping.DefaultLineMapper;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.batch.item.file.transform.DelimitedLineTokenizer;
import org.springframework.batch.item.support.CompositeItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;

import javax.sql.DataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Configuration
public class CsvFileToDatabaseMultiSteps {

    private static final Logger logger = LoggerFactory.getLogger(CsvFileToDatabaseMultiSteps.class);


    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EmployeeCSVtoDatabaseProcessor employeeProcessor;
    private final EmployeeDBWriter employeeDBWriter;
    private final DataSource dataSource;

    @Autowired
    public CsvFileToDatabaseMultiSteps(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory,
                                       EmployeeCSVtoDatabaseProcessor employeeProcessor, EmployeeDBWriter employeeDBWriter,
                                       DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.employeeProcessor = employeeProcessor;
        this.employeeDBWriter = employeeDBWriter;
        this.dataSource = dataSource;
    }

    @Qualifier("csvFileToDatabaseMultiSteps")
    @Bean
    public Job csvFileToDatabaseMultiStepsJob() throws Exception {
        return this.jobBuilderFactory.get("csvFileToDatabaseMultiSteps")
                .start(csvFileToDatabaseMultiStepsStep())
                .next(databaseToCsvFileMultiStepsStep())
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
    public Step csvFileToDatabaseMultiStepsStep() {
        return this.stepBuilderFactory.get("csvFileToDatabaseMultiStepsStep")
                .<EmployeeDTO, Employee>chunk(100)
                .reader(employeeDataMultiStepsReader())
                .processor(employeeProcessor)
                .writer(employeeDBWriter)
                .taskExecutor(taskExecutorMultiSteps())
                .build();
    }

    @Bean
    public Step databaseToCsvFileMultiStepsStep() throws Exception {
        return this.stepBuilderFactory.get("databaseToCsvFileMultiStepsStep")
                .<Employee, EmployeeDTO>chunk(100)
                .reader(employeeDBMultiStepsReader())
                .writer(compositeItemWriter())
                .build();
    }

    @Bean
    @StepScope
    Resource inputDataFileMultiStepsResource(@Value("#{jobParameters[fileName]}") final String fileName) {
        if (fileName == null)
            throw new IllegalArgumentException("The object 'fileName' cannot be null");
        return new ClassPathResource(fileName);
    }

    @Bean
    @StepScope
    public FlatFileItemReader<EmployeeDTO> employeeDataMultiStepsReader() {
        FlatFileItemReader<EmployeeDTO> reader = new FlatFileItemReader<>();
        reader.setResource(inputDataFileMultiStepsResource(""));
        reader.setLineMapper(new DefaultLineMapper<>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames("employeeId", "firstName", "lastName", "email", "age");
            }});
            setFieldSetMapper(new EmployeeFileRawMapper());
        }});
        return reader;
    }

    @Bean
    public TaskExecutor taskExecutorMultiSteps() {
        SimpleAsyncTaskExecutor simpleAsyncTaskExecutor = new SimpleAsyncTaskExecutor();
        simpleAsyncTaskExecutor.setConcurrencyLimit(100);
        return simpleAsyncTaskExecutor;
    }

    @Bean
    @StepScope
    Resource outputFileMultiStepsResource(@Value("#{jobParameters[outputfileRessource]}") final String outputFile) {
        if (outputFile == null)
            throw new IllegalArgumentException("The object 'fileName' cannot be null");
        return new FileSystemResource(outputFile);
    }

    @Bean
    public ItemStreamReader<Employee> employeeDBMultiStepsReader() {
        JdbcCursorItemReader<Employee> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql("select * from employee");
        reader.setRowMapper(new EmployeeDbRawMapper());
        return reader;
    }

    @Bean
    public ItemWriter<EmployeeDTO> employeeFileMultiStepsWriter() {
        FlatFileItemWriter<EmployeeDTO> writer = new FlatFileItemWriter<>();
        writer.setResource(outputFileMultiStepsResource(null));
        writer.setLineAggregator(new DelimitedLineAggregator<>() {
            {
                setFieldExtractor(new BeanWrapperFieldExtractor<>() {{
                    setNames(new String[]{"employeeId", "firstName", "lastName", "email", "age"});
                }});
            }
        });
        writer.setShouldDeleteIfEmpty(true);
        return writer;
    }

    @Bean
    EmailSenderWriter emailSenderWriter() {
        return new EmailSenderWriter();
    }

    @Bean
    public CompositeItemWriter compositeItemWriter() throws Exception {
        CompositeItemWriter compositeItemWriter = new CompositeItemWriter<>();

        List<ItemWriter> itemWriters = new ArrayList<>();

        itemWriters.add(employeeFileMultiStepsWriter());
        itemWriters.add(emailSenderWriter());

        compositeItemWriter.setDelegates(itemWriters);
        compositeItemWriter.afterPropertiesSet();

        return compositeItemWriter;
    }
}
