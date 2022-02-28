package com.arcadia.whiteRabbitService.model.scandata;

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
@Entity(name = "scan_data_params")
public class ScanDataParams {
    @Id
    @SequenceGenerator(name = "scan_data_param_id_sequence", sequenceName = "scan_data_param_id_sequence")
    @GeneratedValue(strategy = SEQUENCE, generator = "scan_data_param_id_sequence")
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
    @OneToOne(mappedBy = "scanDataParams", fetch = FetchType.LAZY)
    private ScanDbSetting scanDbSettings;

    @JsonIgnore
    @OneToOne(mappedBy = "scanDataParams", fetch = FetchType.LAZY)
    private ScanFilesSettings scanFilesSettings;
}
