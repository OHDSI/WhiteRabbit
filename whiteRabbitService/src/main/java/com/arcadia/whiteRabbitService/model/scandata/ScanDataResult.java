package com.arcadia.whiteRabbitService.model.scandata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

import java.sql.Timestamp;
import java.util.Objects;

import static javax.persistence.CascadeType.ALL;
import static javax.persistence.FetchType.LAZY;
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

    @Column(name = "file_id", nullable = false)
    private Long fileId;

    @JsonIgnore
    @OneToOne(optional = false, fetch = LAZY)
    @JoinColumn(name = "scan_data_conversion_id", referencedColumnName = "id", nullable = false)
    private ScanDataConversion scanDataConversion;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScanDataResult that = (ScanDataResult) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
