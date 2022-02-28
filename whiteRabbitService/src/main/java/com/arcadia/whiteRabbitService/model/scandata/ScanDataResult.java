package com.arcadia.whiteRabbitService.model.scandata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

import java.sql.Timestamp;

import static javax.persistence.CascadeType.ALL;
import static javax.persistence.GenerationType.SEQUENCE;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity(name = "scan_data_results")
public class ScanDataResult {
    @Id
    @SequenceGenerator(name = "scan_data_result_id_sequence", sequenceName = "scan_data_result_id_sequence")
    @GeneratedValue(strategy = SEQUENCE, generator = "scan_data_result_id_sequence")
    private Long id;

    @Column(nullable = false)
    private Timestamp time;

    @Column(name = "file_name", nullable = false)
    private String fileName;

    @Column(name = "file_key")
    private String fileKey;

    @JsonIgnore
    @OneToOne(cascade = ALL, optional = false)
    @JoinColumn(name = "scan_data_conversion_id", referencedColumnName = "id")
    private ScanDataConversion scanDataConversion;
}
