package com.arcadia.whiteRabbitService.model.scandb;

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
@Entity(name = "scan_files_settings")
public class ScanFilesSettings implements ScanSettings {
    @Id
    @SequenceGenerator(name = "scan_files_settings_id_sequence", sequenceName = "scan_files_settings_id_sequence")
    @GeneratedValue(strategy = SEQUENCE, generator = "scan_files_settings_id_sequence")
    private Long id;

    @NonNull
    @Column(name = "file_type", nullable = false)
    private String fileType;

    @Column(nullable = false)
    private String delimiter;

    @OneToOne(cascade = ALL, fetch = LAZY, optional = false)
    @JoinColumn(name = "scan_conversion_id", referencedColumnName = "id")
    private ScanConversion scanConversion;

    @OneToOne(cascade = ALL, fetch = LAZY, optional = false)
    @JoinColumn(name = "scan_params_id", referencedColumnName = "id")
    private ScanParams scanParams;
}
