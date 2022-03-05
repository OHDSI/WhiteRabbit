package com.arcadia.whiteRabbitService.model.scandata;

import lombok.*;
import org.ohdsi.whiteRabbit.DbSettings;

import javax.persistence.*;
import javax.validation.constraints.NotNull;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

import static com.arcadia.whiteRabbitService.util.DbSettingsAdapter.adaptDelimitedTextFileSettings;
import static com.arcadia.whiteRabbitService.util.FileUtil.deleteRecursive;
import static javax.persistence.CascadeType.ALL;
import static javax.persistence.CascadeType.PERSIST;
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

    @NotNull
    @Column(name = "file_type", nullable = false)
    private String fileType;

    @NotNull
    @Column(nullable = false)
    private String delimiter;

    @Transient
    private List<String> fileNames;

    @Transient
    private String directory;

    @OneToOne(fetch = LAZY, optional = false)
    @JoinColumn(name = "scan_data_conversion_id", referencedColumnName = "id")
    private ScanDataConversion scanDataConversion;

    @NotNull
    @OneToOne(cascade = PERSIST, fetch = LAZY, optional = false)
    @JoinColumn(name = "scan_params_id", referencedColumnName = "id")
    private ScanDataParams scanDataParams;

    @Override
    public DbSettings toWhiteRabbitSettings() {
        return adaptDelimitedTextFileSettings(this);
    }

    @SneakyThrows
    @Override
    public void destroy() {
        deleteRecursive(Path.of(directory));
    }

    @Override
    public String scanReportFileName() {
        return "scan-report.xlsx";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ScanFilesSettings that = (ScanFilesSettings) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
