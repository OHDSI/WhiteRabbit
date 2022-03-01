package com.arcadia.whiteRabbitService.model.scandata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.ohdsi.whiteRabbit.DbSettings;
import org.springframework.lang.NonNull;

import javax.persistence.*;

import static com.arcadia.whiteRabbitService.util.DbSettingsAdapter.adaptDbSettings;
import static javax.persistence.CascadeType.ALL;
import static javax.persistence.FetchType.LAZY;
import static javax.persistence.GenerationType.SEQUENCE;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity(name = "scan_db_settings")
public class ScanDbSetting implements ScanDataSettings {
    @Id
    @SequenceGenerator(name = "scan_db_setting_id_sequence", sequenceName = "scan_db_setting_id_sequence")
    @GeneratedValue(strategy = SEQUENCE, generator = "scan_db_setting_id_sequence")
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
    @Transient
    private String tablesToScan;

    @JsonIgnore
    @OneToOne(fetch = LAZY, optional = false)
    @JoinColumn(name = "scan_data_conversion_id", referencedColumnName = "id")
    private ScanDataConversion scanDataConversion;

    @OneToOne(cascade = ALL, fetch = LAZY, optional = false, orphanRemoval = true)
    @JoinColumn(name = "scan_params_id", referencedColumnName = "id")
    private ScanDataParams scanDataParams;

    @Override
    public DbSettings toWhiteRabbitSettings() {
        return adaptDbSettings(this);
    }

    @Override
    public String scanReportFileName() {
        return database + ".xlsx";
    }
}
