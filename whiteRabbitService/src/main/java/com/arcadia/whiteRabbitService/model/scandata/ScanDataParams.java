package com.arcadia.whiteRabbitService.model.scandata;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import javax.validation.constraints.NotNull;

import java.util.Objects;

import static javax.persistence.GenerationType.SEQUENCE;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity(name = "scan_data_params")
public class ScanDataParams {
    @Id
    @SequenceGenerator(name = "scan_data_param_id_sequence", sequenceName = "scan_data_param_id_sequence")
    @GeneratedValue(strategy = SEQUENCE, generator = "scan_data_param_id_sequence")
    private Long id;

    @NotNull
    @Column(name = "scan_values", nullable = false)
    private Boolean scanValues;

    @Column(name = "min_cell_count")
    private Integer minCellCount;

    @Column(name = "max_values")
    private Integer maxValues;

    @Column(name = "sample_size")
    private Integer sampleSize;

    @NotNull
    @Column(name = "calculate_numeric_stats", nullable = false)
    private Boolean calculateNumericStats;

    @Column(name = "numeric_stats_sampler_size")
    private Integer numericStatsSamplerSize;

    @JsonIgnore
    @OneToOne(mappedBy = "scanDataParams", fetch = FetchType.LAZY)
    private ScanDbSettings scanDbSettings;

    @JsonIgnore
    @OneToOne(mappedBy = "scanDataParams", fetch = FetchType.LAZY)
    private ScanFilesSettings scanFilesSettings;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScanDataParams that = (ScanDataParams) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
