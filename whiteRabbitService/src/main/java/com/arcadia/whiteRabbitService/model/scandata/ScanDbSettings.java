package com.arcadia.whiteRabbitService.model.scandata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.ohdsi.whiteRabbit.DbSettings;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.util.Objects;

import static com.arcadia.whiteRabbitService.util.DbSettingsAdapter.adaptDbSettings;
import static javax.persistence.CascadeType.PERSIST;
import static javax.persistence.FetchType.LAZY;
import static javax.persistence.GenerationType.SEQUENCE;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity(name = "scan_db_settings")
public class ScanDbSettings implements ScanDataSettings {
    @Id
    @SequenceGenerator(name = "scan_db_setting_id_sequence", sequenceName = "scan_db_setting_id_sequence")
    @GeneratedValue(strategy = SEQUENCE, generator = "scan_db_setting_id_sequence")
    private Long id;

    @NotNull
    @Column(nullable = false)
    private String dbType;

    @NotNull
    @Column(nullable = false)
    private String server;

    @NotNull
    @Column(nullable = false)
    private Integer port;

    @NotNull
    @Column(name = "username", nullable = false)
    private String user;

    @NotNull
    @Transient
    private String password;

    @NotNull
    @Column(name = "database_name", nullable = false)
    private String database;

    @Column(name = "schema_name")
    private String schema;

    @Transient
    private String tablesToScan;

    @JsonIgnore
    @OneToOne(fetch = LAZY, optional = false)
    @JoinColumn(name = "scan_data_conversion_id", referencedColumnName = "id")
    private ScanDataConversion scanDataConversion;

    @OneToOne(cascade = PERSIST, fetch = LAZY, optional = false, orphanRemoval = true)
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScanDbSettings that = (ScanDbSettings) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
