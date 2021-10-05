package com.starapp.springbatch.job;

import com.starapp.springbatch.dto.EmployeeDTO;
import com.starapp.springbatch.mapper.EmployeeFileRawMapper;
import com.starapp.springbatch.model.Employee;
import com.starapp.springbatch.processor.EmployeeProcessor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.JobBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepBuilderFactory;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.item.database.BeanPropertyItemSqlParameterSourceProvider;
import org.springframework.batch.item.database.JdbcBatchItemWriter;
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

import javax.sql.DataSource;


@Configuration
public class Demo1 {

    private final JobBuilderFactory jobBuilderFactory;
    private final StepBuilderFactory stepBuilderFactory;
    private final EmployeeProcessor employeeProcessor;
    private final DataSource dataSource;

    @SuppressWarnings("SpringElInspection")
    @Value("#{jobParameters[fileName]}")
    private String fileName;

    @Autowired
    public Demo1(JobBuilderFactory jobBuilderFactory, StepBuilderFactory stepBuilderFactory, EmployeeProcessor employeeProcessor, DataSource dataSource) {
        this.jobBuilderFactory = jobBuilderFactory;
        this.stepBuilderFactory = stepBuilderFactory;
        this.employeeProcessor = employeeProcessor;
        this.dataSource = dataSource;
    }

    @Qualifier("demo1")
    @Bean
    public Job demo1Job() {
        return this.jobBuilderFactory.get("demo1")
                .start(step1Demo())
                .build();
    }

    @Bean
    public Step step1Demo() {
        return this.stepBuilderFactory.get("step1")
                .<EmployeeDTO, Employee>chunk(5)
                .reader(employeeReader())
                .processor(employeeProcessor)
                .writer(employeeDBWriterDefault())
                .build();
    }

    @Bean
    @StepScope
    Resource inputFileResource() {
        if (fileName == null)
            throw new IllegalArgumentException("The object 'fileName' cannot be null");
        return new ClassPathResource(fileName);
    }

    @Bean
    @StepScope
    public FlatFileItemReader<EmployeeDTO> employeeReader() {
        FlatFileItemReader<EmployeeDTO> reader = new FlatFileItemReader<>();
        reader.setResource(inputFileResource());
        reader.setLineMapper(new DefaultLineMapper<>() {{
            setLineTokenizer(new DelimitedLineTokenizer() {{
                setNames("employeeId", "firstName", "lastName", "email", "age");
            }});
            setFieldSetMapper(new EmployeeFileRawMapper());
        }});
        return reader;
    }

    private JdbcBatchItemWriter<Employee> employeeDBWriterDefault() {
        JdbcBatchItemWriter<Employee> itemWriter = new JdbcBatchItemWriter<>();
        itemWriter.setDataSource(dataSource);
        itemWriter.setSql("insert into employee (employee_id, first_name, last_name, email, age) values (:employeeId, :firstName, :lastName, :email, :age)");
        itemWriter.setItemSqlParameterSourceProvider(new BeanPropertyItemSqlParameterSourceProvider<>());

        itemWriter.afterPropertiesSet();
        return itemWriter;
    }

}
