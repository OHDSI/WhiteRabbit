package com.arcadia.whiteRabbitService.model.fakedata;

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

import static javax.persistence.CascadeType.ALL;
import static javax.persistence.CascadeType.PERSIST;
import static javax.persistence.FetchType.LAZY;
import static javax.persistence.GenerationType.SEQUENCE;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity(name = "fake_data_conversion")
public class FakeDataConversion implements Conversion<FakeDataLog> {
    @Id
    @SequenceGenerator(name = "fake_data_conversion_id_sequence", sequenceName = "fake_data_conversion_id_sequence")
    @GeneratedValue(strategy = SEQUENCE, generator = "fake_data_conversion_id_sequence")
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
    @OneToMany(mappedBy = "fakeDataConversion", fetch = LAZY, orphanRemoval = true)
    private List<FakeDataLog> logs;

    @JsonIgnore
    @OneToOne(cascade = PERSIST, mappedBy = "fakeDataConversion", fetch = LAZY, orphanRemoval = true)
    private FakeDataSettings fakeDataSettings;

    @JsonIgnore
    @Override
    public void setStatus(ConversionStatus status) {
        statusCode = status.getCode();
        statusName = status.getName();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FakeDataConversion that = (FakeDataConversion) o;
        return Objects.equals(id, that.id);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id);
    }
}
