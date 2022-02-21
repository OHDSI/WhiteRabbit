package com.arcadia.whiteRabbitService.model.scandb;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

import static javax.persistence.CascadeType.ALL;
import static javax.persistence.GenerationType.SEQUENCE;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity(name = "scan_results")
public class ScanResult {
    @Id
    @SequenceGenerator(name = "scan_result_id_sequence", sequenceName = "scan_result_id_sequence")
    @GeneratedValue(strategy = SEQUENCE, generator = "scan_result_id_sequence")
    private Long id;

    @Column(name = "file_id", nullable = false)
    private String fileId;

    @JsonIgnore
    @OneToOne(cascade = ALL)
    @JoinColumn(name = "scan_conversion_id", referencedColumnName = "id")
    private ScanConversion scanConversion;
}
