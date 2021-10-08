package com.starapp.springbatch.processor;

import com.starapp.springbatch.dto.EmployeeDTO;
import com.starapp.springbatch.model.Employee;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class EmployeeDatabaseToCSVProcessor implements ItemProcessor<Employee, EmployeeDTO> {

    private static final Logger logger = LoggerFactory.getLogger(EmployeeCSVtoDatabaseProcessor.class);

    @Override
    public EmployeeDTO process(Employee employee) {
        logger.info("Inside process method = {}", employee.toString());
        if (!isValid(employee))
            return null;
        EmployeeDTO employeeDto = new EmployeeDTO();
        employeeDto.setEmployeeId(employee.getEmployeeId());
        employeeDto.setFirstName(employee.getFirstName());
        employeeDto.setLastName(employee.getLastName());
        employeeDto.setEmail(employee.getEmail());
        employeeDto.setAge(employee.getAge());
        return employeeDto;
    }

    boolean isValid(Employee employee) {
        return employee.getFirstName().startsWith("N");
    }
}
