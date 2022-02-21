package com.arcadia.whiteRabbitService.model.scandb;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.lang.NonNull;

import javax.persistence.*;

import static javax.persistence.CascadeType.ALL;
import static javax.persistence.FetchType.LAZY;
import static javax.persistence.GenerationType.SEQUENCE;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity(name = "scan_db_settings")
public class ScanDbSettings implements ScanSettings {
    @Id
    @SequenceGenerator(name = "scan_db_settings_id_sequence", sequenceName = "scan_db_settings_id_sequence")
    @GeneratedValue(strategy = SEQUENCE, generator = "scan_db_settings_id_sequence")
    private Long id;

    @NonNull
    @Column(nullable = false)
    private String dbType;

    @NonNull
    @Column(nullable = false)
    private String server;

    @NonNull
    @Column(nullable = false)
    private Integer port;

    @NonNull
    @Column(name = "username", nullable = false)
    private String user;

    @NonNull
    @Transient
    private String password;

    @NonNull
    @Column(name = "database_name", nullable = false)
    private String database;

    @Column(name = "schema_name")
    private String schema;

    @NonNull
    @Column(name = "tables_to_scan", nullable = false)
    private String tablesToScan;

    @JsonIgnore
    @OneToOne(fetch = LAZY, optional = false)
    @JoinColumn(name = "scan_conversion_id", referencedColumnName = "id")
    private ScanConversion scanConversion;

    @OneToOne(cascade = ALL, fetch = LAZY, optional = false)
    @JoinColumn(name = "scan_params_id", referencedColumnName = "id")
    private ScanParams scanParams;
}
