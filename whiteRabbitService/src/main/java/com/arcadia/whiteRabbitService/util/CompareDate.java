package com.arcadia.whiteRabbitService.util;

import java.util.Date;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class CompareDate {

    public static long getDateDiffInHours(Date first, Date second) {
        long diffInMilliseconds = Math.abs(first.getTime() - second.getTime());

        return HOURS.convert(diffInMilliseconds, MILLISECONDS);
    }
}
