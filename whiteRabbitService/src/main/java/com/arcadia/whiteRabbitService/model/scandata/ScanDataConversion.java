package com.arcadia.whiteRabbitService.model.scandata;

import com.arcadia.whiteRabbitService.model.ConversionStatus;
import com.fasterxml.jackson.annotation.JsonIgnore;
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
@Entity(name = "scan_data_conversions")
public class ScanDataConversion {
    @Id
    @SequenceGenerator(name = "scan_data_conversion_id_sequence", sequenceName = "scan_data_conversion_id_sequence")
    @GeneratedValue(strategy = SEQUENCE, generator = "scan_data_conversion_id_sequence")
    private Long id;

    @Column(name = "username", nullable = false)
    private String username;

    @Column(name = "project", nullable = false)
    private String project;

    @Column(name = "status_code", nullable = false)
    private Integer statusCode;

    @Column(name = "status_name", nullable = false)
    private String statusName;

    @JsonIgnore
    @OneToMany(mappedBy = "scanDataConversion", fetch = LAZY)
    private List<ScanDataLog> logs;

    @JsonIgnore
    @OneToOne(cascade = ALL, mappedBy = "scanDataConversion", fetch = LAZY, orphanRemoval = true)
    private ScanDataResult result;

    @JsonIgnore
    @OneToOne(cascade = ALL, mappedBy = "scanDataConversion", fetch = LAZY, orphanRemoval = true)
    private ScanDbSetting dbSettings;

    @JsonIgnore
    @OneToOne(cascade = ALL, mappedBy = "scanDataConversion", fetch = LAZY, orphanRemoval = true)
    private ScanFilesSettings filesSettings;

    @JsonIgnore
    public ScanDataSettings getSettings() {
        if (dbSettings != null) {
            return dbSettings;
        } else if (filesSettings != null) {
            return filesSettings;
        } else {
            throw new RuntimeException("Can not extract settings for conversion with id " + id);
        }
    }

    @JsonIgnore
    public void setStatus(ConversionStatus status) {
        statusCode = status.getCode();
        statusName = status.getName();
    }
}
