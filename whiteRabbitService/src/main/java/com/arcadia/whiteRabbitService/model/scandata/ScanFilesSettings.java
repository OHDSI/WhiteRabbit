package com.arcadia.whiteRabbitService.model.scandata;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.ohdsi.whiteRabbit.DbSettings;
import org.springframework.lang.NonNull;

import javax.persistence.*;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

import static com.arcadia.whiteRabbitService.util.DbSettingsAdapter.adaptDelimitedTextFileSettings;
import static com.arcadia.whiteRabbitService.util.FileUtil.deleteRecursive;
import static javax.persistence.CascadeType.ALL;
import static javax.persistence.FetchType.LAZY;
import static javax.persistence.GenerationType.SEQUENCE;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity(name = "scan_files_settings")
public class ScanFilesSettings implements ScanDataSettings {
    @Id
    @SequenceGenerator(name = "scan_files_setting_id_sequence", sequenceName = "scan_files_setting_id_sequence")
    @GeneratedValue(strategy = SEQUENCE, generator = "scan_files_setting_id_sequence")
    private Long id;

    @NonNull
    @Column(name = "file_type", nullable = false)
    private String fileType;

    @Column(nullable = false)
    private String delimiter;

    @Transient
    private List<String> fileNames;

    @Transient
    private String directory;

    @Transient
    private String project;

    @OneToOne(cascade = ALL, fetch = LAZY, optional = false)
    @JoinColumn(name = "scan_data_conversion_id", referencedColumnName = "id")
    private ScanDataConversion scanDataConversion;

    @OneToOne(cascade = ALL, fetch = LAZY, optional = false)
    @JoinColumn(name = "scan_params_id", referencedColumnName = "id")
    private ScanDataParams scanDataParams;

    @Override
    public DbSettings toWhiteRabbitSettings() {
        return adaptDelimitedTextFileSettings(this);
    }

    @Override
    public void destroy() {
        deleteRecursive(Path.of(directory));
    }

    @Override
    public String scanReportFileName() {
        return "scan-report.xlsx";
    }
}
