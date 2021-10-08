package com.starapp.springbatch.job;

import com.starapp.springbatch.dto.EmployeeDTO;
import com.starapp.springbatch.mapper.EmployeeDbRawMapper;
import com.starapp.springbatch.model.Employee;
import com.starapp.springbatch.processor.EmployeeProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.ItemStreamReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JdbcCursorItemReader;
import org.springframework.batch.item.file.FlatFileItemWriter;
import org.springframework.batch.item.file.transform.BeanWrapperFieldExtractor;
import org.springframework.batch.item.file.transform.DelimitedLineAggregator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;

import javax.sql.DataSource;

@Configuration
public class DatabaseToCsvFile {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EmployeeProcessor employeeProcessor;
    private final DataSource dataSource;
    private final Resource outputResource = new FileSystemResource("output/employee_output.csv");

    @Autowired
    public DatabaseToCsvFile(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory,
                             EmployeeProcessor employeeProcessor, DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.employeeProcessor = employeeProcessor;
        this.dataSource = dataSource;
    }

    @Qualifier("databaseToCsvFile")
    @Bean
    public Job databaseToCsvFileJob() {
        return this.jobBuilderFactory.get("databaseToCsvFile")
                .start(databaseToCsvFileStep())
                .build();
    }

    @Bean
    public Step databaseToCsvFileStep() {
        return this.stepBuilderFactory.get("databaseToCsvFileStep")
                .<Employee, EmployeeDTO>chunk(10)
                .reader(employeeDBReader())
                .writer(employeeFileWriter())
                .build();
    }

    @Bean
    public ItemStreamReader<Employee> employeeDBReader() {
        JdbcCursorItemReader<Employee> reader = new JdbcCursorItemReader<>();
        reader.setDataSource(dataSource);
        reader.setSql("select * from employee");
        reader.setRowMapper(new EmployeeDbRawMapper());
        return reader;
    }


    @Bean
    @StepScope
    Resource outputFileResource(@Value("#{jobParameters[outputfileRessource]}") final String outputFile) {
        if (outputFile == null)
            throw new IllegalArgumentException("The object 'fileName' cannot be null");
        return new FileSystemResource(outputFile);
    }

    @Bean
    public ItemWriter<EmployeeDTO> employeeFileWriter() {
        FlatFileItemWriter<EmployeeDTO> writer = new FlatFileItemWriter<>();
        writer.setResource(outputFileResource(null));
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

}
