package com.arcadia.whiteRabbitService.model.fakedata;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.Id;
import javax.persistence.SequenceGenerator;

import static javax.persistence.GenerationType.SEQUENCE;

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
}
