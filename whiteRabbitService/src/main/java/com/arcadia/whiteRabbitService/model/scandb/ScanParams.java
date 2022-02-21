package com.arcadia.whiteRabbitService.model.scandb;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.lang.NonNull;

import javax.persistence.*;

import static javax.persistence.GenerationType.SEQUENCE;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity(name = "scan_params")
public class ScanParams {
    @Id
    @SequenceGenerator(name = "scan_params_id_sequence", sequenceName = "scan_params_id_sequence")
    @GeneratedValue(strategy = SEQUENCE, generator = "scan_params_id_sequence")
    private Long id;

    @NonNull
    @Column(name = "scan_values", nullable = false)
    private Boolean scanValues;

    @Column(name = "min_cell_count")
    private Integer minCellCount;

    @Column(name = "max_values")
    private Integer maxValues;

    @Column(name = "sample_size")
    private Integer sampleSize;

    @NonNull
    @Column(name = "calculate_numeric_stats", nullable = false)
    private Boolean calculateNumericStats;

    @Column(name = "numeric_stats_sampler_size")
    private Integer numericStatsSamplerSize;

    @JsonIgnore
    @OneToOne(mappedBy = "scanParams", fetch = FetchType.LAZY)
    private ScanDbSettings scanDbSettings;

    @JsonIgnore
    @OneToOne(mappedBy = "scanParams", fetch = FetchType.LAZY)
    private ScanFilesSettings scanFilesSettings;
}
