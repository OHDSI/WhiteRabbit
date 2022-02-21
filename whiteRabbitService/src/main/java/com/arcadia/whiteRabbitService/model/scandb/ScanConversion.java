package com.arcadia.whiteRabbitService.model.scandb;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

import java.util.List;

import static javax.persistence.CascadeType.ALL;
import static javax.persistence.FetchType.LAZY;
import static javax.persistence.GenerationType.SEQUENCE;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity(name = "scan_conversions")
public class ScanConversion {
    @Id
    @SequenceGenerator(name = "scan_conversion_id_sequence", sequenceName = "scan_conversion_id_sequence")
    @GeneratedValue(strategy = SEQUENCE, generator = "scan_conversion_id_sequence")
    private Long id;

    @Column(name = "username", nullable = false)
    private String username;

    @Column(name = "project", nullable = false)
    private String project;

    @OneToMany(mappedBy = "scanConversion", fetch = LAZY)
    private List<ScanLog> logs;

    @OneToOne(cascade = ALL, mappedBy = "scanConversion", fetch = LAZY)
    private ScanResult scanResult;

    @OneToOne(cascade = ALL, mappedBy = "scanConversion", fetch = LAZY)
    private ScanDbSettings scanDbSettings;

    @OneToOne(cascade = ALL, mappedBy = "scanConversion", fetch = LAZY)
    private ScanFilesSettings scanFilesSettings;
}
