package com.starapp.springbatch.mapper;

import com.starapp.springbatch.dto.EmployeeDTO;
import org.springframework.batch.item.file.mapping.FieldSetMapper;
import org.springframework.batch.item.file.transform.FieldSet;

public class EmployeeFileRawMapper implements FieldSetMapper<EmployeeDTO> {
    @Override
    public EmployeeDTO mapFieldSet(FieldSet fieldSet) {
        EmployeeDTO employeeDTO = new EmployeeDTO();
        employeeDTO.setEmployeeId(fieldSet.readString("employeeId"));
        employeeDTO.setFirstName(fieldSet.readString("firstName"));
        employeeDTO.setLastName(fieldSet.readString("lastName"));
        employeeDTO.setEmail(fieldSet.readString("email"));
        try {
            employeeDTO.setAge(fieldSet.readInt("age"));
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return employeeDTO;
    }
}
