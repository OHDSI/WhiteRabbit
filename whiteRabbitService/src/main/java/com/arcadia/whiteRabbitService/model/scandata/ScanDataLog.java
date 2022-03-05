package com.arcadia.whiteRabbitService.model.scandata;

import com.arcadia.whiteRabbitService.model.Log;
import com.arcadia.whiteRabbitService.model.LogStatus;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;

import java.sql.Timestamp;
import java.util.Objects;

import static javax.persistence.FetchType.LAZY;
import static javax.persistence.GenerationType.SEQUENCE;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity(name = "scan_data_logs")
public class ScanDataLog implements Log {
    @Id
    @SequenceGenerator(name = "scan_data_log_id_sequence", sequenceName = "scan_data_log_id_sequence")
    @GeneratedValue(strategy = SEQUENCE, generator = "scan_data_log_id_sequence")
    private Long id;

    @Column(nullable = false, length = 1000)
    private String message;

    @Column(nullable = false)
    private Timestamp time;

    @Column(name = "status_code", nullable = false)
    private Integer statusCode;

    @Column(name = "status_name", nullable = false, length = 25)
    private String statusName;

    @Column(name = "percent", nullable = false)
    private Integer percent;

    @JsonIgnore
    @ManyToOne(optional = false, fetch = LAZY)
    @JoinColumn(name = "conversion_id")
    private ScanDataConversion scanDataConversion;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScanDataLog that = (ScanDataLog) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
