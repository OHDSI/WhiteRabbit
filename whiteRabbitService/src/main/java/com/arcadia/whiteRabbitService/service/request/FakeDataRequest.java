package com.arcadia.whiteRabbitService.service.request;

import com.arcadia.whiteRabbitService.model.fakedata.FakeDataSettings;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotNull;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FakeDataRequest {
    @NotNull
    FakeDataSettings settings;
    @NotNull
    ScanReportRequest scanReportInfo;
}
