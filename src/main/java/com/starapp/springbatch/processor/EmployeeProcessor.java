package com.starapp.springbatch.processor;

import com.starapp.springbatch.dto.EmployeeDTO;
import com.starapp.springbatch.model.Employee;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

@Component
public class EmployeeProcessor implements ItemProcessor<EmployeeDTO, Employee> {
    @Override
    public Employee process(EmployeeDTO employeeDTO) {
        Employee employee = new Employee();
        employee.setEmployeeId(employeeDTO.getEmployeeId());
        employee.setFirstName(employeeDTO.getFirstName());
        employee.setLastName(employeeDTO.getLastName());
        employee.setEmail(employeeDTO.getEmail());
        employee.setAge(employeeDTO.getAge());
        return employee;
    }
}
