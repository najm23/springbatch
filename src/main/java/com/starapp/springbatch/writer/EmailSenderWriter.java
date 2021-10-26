package com.starapp.springbatch.writer;

import com.starapp.springbatch.model.Employee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;

import java.util.List;

public class EmailSenderWriter implements ItemWriter<Employee> {

    public static final Logger logger = LoggerFactory.getLogger(EmailSenderWriter.class);


    @Override
    public void write(List<? extends Employee> items) throws Exception {

        for (Employee employeeDTO : items) {
            logger.info("Email send succesfully to {}", employeeDTO.getEmail());
        }
    }
}
