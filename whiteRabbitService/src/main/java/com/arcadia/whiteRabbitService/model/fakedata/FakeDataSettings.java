package com.arcadia.whiteRabbitService.model.fakedata;

import com.arcadia.whiteRabbitService.model.scandata.ScanDbSettings;
import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.*;
import javax.validation.constraints.NotNull;

import java.nio.file.Path;
import java.util.Objects;

import static com.arcadia.whiteRabbitService.util.FileUtil.deleteRecursive;
import static javax.persistence.FetchType.LAZY;
import static javax.persistence.GenerationType.SEQUENCE;

/**
 * Not supported generation fake data by CSV files
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity(name = "fake_data_settings")
public class FakeDataSettings {
    @Id
    @SequenceGenerator(name = "fake_data_settings_id_sequence", sequenceName = "fake_data_settings_id_sequence")
    @GeneratedValue(strategy = SEQUENCE, generator = "fake_data_settings_id_sequence")
    private Long id;

    @NotNull
    @Column(name = "max_row_count", nullable = false)
    private Integer maxRowCount;

    @NotNull
    @Column(name = "do_uniform_sampling", nullable = false)
    private Boolean doUniformSampling;

    @NotNull
    @Transient
    private String userSchema;

    @Transient
    private String directory;

    @Transient
    private String scanReportFileName;

    @JsonIgnore
    @OneToOne(fetch = LAZY, optional = false)
    @JoinColumn(name = "fake_data_conversion_id", referencedColumnName = "id")
    private FakeDataConversion fakeDataConversion;

    public void destroy() {
        deleteRecursive(Path.of(directory));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FakeDataSettings that = (FakeDataSettings) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
