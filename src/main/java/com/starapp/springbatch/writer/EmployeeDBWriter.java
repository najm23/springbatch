package com.starapp.springbatch.writer;

import com.starapp.springbatch.model.Employee;
import com.starapp.springbatch.repo.EmployeeRepo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemWriter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
public class EmployeeDBWriter implements ItemWriter<Employee> {

    public static final Logger logger = LoggerFactory.getLogger(EmployeeDBWriter.class);

    private final EmployeeRepo employeeRepo;

    @Autowired
    public EmployeeDBWriter(EmployeeRepo employeeRepo) {
        this.employeeRepo = employeeRepo;
    }

    @Override
    public void write(List<? extends Employee> employees) {
        employeeRepo.saveAll(employees);
        logger.info("{} employees saved in database", employees.size());
    }
}
