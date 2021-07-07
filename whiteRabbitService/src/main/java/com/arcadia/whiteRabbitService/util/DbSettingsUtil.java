package com.arcadia.whiteRabbitService.util;

import com.arcadia.whiteRabbitService.dto.DbSettingsDto;
import com.arcadia.whiteRabbitService.dto.FileSettingsDto;
import com.arcadia.whiteRabbitService.dto.SettingsDto;
import com.arcadia.whiteRabbitService.service.error.DbTypeNotSupportedException;
import com.arcadia.whiteRabbitService.service.error.DelimitedTextFileNotSupportedException;
import org.ohdsi.whiteRabbit.DbSettings;

import static com.arcadia.whiteRabbitService.service.DbSettingsAdapter.adaptDbSettings;
import static com.arcadia.whiteRabbitService.service.DbSettingsAdapter.adaptDelimitedTextFileSettings;

public class DbSettingsUtil {
    public static DbSettings dtoToDbSettings(SettingsDto dto) throws DbTypeNotSupportedException, DelimitedTextFileNotSupportedException {
        return dto instanceof DbSettingsDto ?
                dbDtoToSettings((DbSettingsDto) dto) :
                fileDtoToSettings((FileSettingsDto) dto);
    }

    private static DbSettings fileDtoToSettings(FileSettingsDto dto) throws DelimitedTextFileNotSupportedException {
        return adaptDelimitedTextFileSettings(dto);
    }

    private static DbSettings dbDtoToSettings(DbSettingsDto dto) throws DbTypeNotSupportedException {
        return adaptDbSettings(dto);
    }
}
