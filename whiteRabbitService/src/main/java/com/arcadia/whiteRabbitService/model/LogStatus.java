package com.arcadia.whiteRabbitService.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;

import static javax.persistence.GenerationType.SEQUENCE;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity(name = "log_statuses")
public class LogStatus {
    @Id
    @SequenceGenerator(name = "log_statuses_id_sequence", sequenceName = "log_statuses_id_sequence")
    @GeneratedValue(strategy = SEQUENCE, generator = "log_statuses_id_sequence")
    private Long id;

    private Integer code;

    private String description;
}
