package com.arcadia.whiteRabbitService.model.scandb;

import com.arcadia.whiteRabbitService.model.LogStatus;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

import java.sql.Timestamp;

import static javax.persistence.GenerationType.SEQUENCE;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity(name = "scan_logs")
public class ScanLog {
    @Id
    @SequenceGenerator(name = "scan_log_id_sequence", sequenceName = "scan_log_id_sequence")
    @GeneratedValue(strategy = SEQUENCE, generator = "scan_log_id_sequence")
    private Long id;

    @Column(nullable = false)
    private String message;

    @Column(nullable = false)
    private Timestamp time;

    @ManyToOne(optional = false)
    @JoinColumn(name = "status_id")
    private LogStatus status;

    @JsonIgnore
    @ManyToOne(optional = false)
    @JoinColumn(name = "conversion_id")
    private ScanConversion scanConversion;
}
