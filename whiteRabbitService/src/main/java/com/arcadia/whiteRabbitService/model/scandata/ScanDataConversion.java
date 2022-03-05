package com.arcadia.whiteRabbitService.model.scandata;

import com.arcadia.whiteRabbitService.model.Conversion;
import com.arcadia.whiteRabbitService.model.ConversionStatus;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import java.util.List;
import java.util.Objects;

import static javax.persistence.CascadeType.*;
import static javax.persistence.FetchType.LAZY;
import static javax.persistence.GenerationType.SEQUENCE;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity(name = "scan_data_conversions")
public class ScanDataConversion implements Conversion<ScanDataLog> {
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

    @Column(name = "status_name", nullable = false, length = 25)
    private String statusName;

    @JsonIgnore
    @OneToMany(mappedBy = "scanDataConversion", fetch = LAZY, orphanRemoval = true)
    private List<ScanDataLog> logs;

    @JsonIgnore
    @OneToOne(mappedBy = "scanDataConversion", fetch = LAZY, orphanRemoval = true)
    private ScanDataResult result;

    @JsonIgnore
    @OneToOne(cascade = PERSIST, mappedBy = "scanDataConversion", fetch = LAZY, orphanRemoval = true)
    private ScanDbSettings dbSettings;

    @JsonIgnore
    @OneToOne(cascade = PERSIST, mappedBy = "scanDataConversion", fetch = LAZY, orphanRemoval = true)
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

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScanDataConversion that = (ScanDataConversion) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
