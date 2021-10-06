package com.starapp.springbatch.processor;

import com.starapp.springbatch.dto.EmployeeDTO;
import com.starapp.springbatch.model.Employee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class EmployeeProcessor implements ItemProcessor<EmployeeDTO, Employee> {

    private static final Logger logger = LoggerFactory.getLogger(EmployeeProcessor.class);

    @Override
    public Employee process(EmployeeDTO employeeDTO) {
        logger.info("Inside process method = {}", employeeDTO.toString());
        if (!isValid(employeeDTO))
            return null;
        Employee employee = new Employee();
        employee.setEmployeeId(employeeDTO.getEmployeeId());
        employee.setFirstName(employeeDTO.getFirstName());
        employee.setLastName(employeeDTO.getLastName());
        employee.setEmail(employeeDTO.getEmail());
        employee.setAge(employeeDTO.getAge());
        return employee;
    }

    boolean isValid(EmployeeDTO employeeDTO) {
        return employeeDTO.getFirstName().startsWith("N");
    }
}
