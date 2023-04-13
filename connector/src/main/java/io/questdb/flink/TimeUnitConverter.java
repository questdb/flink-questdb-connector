package io.questdb.flink;

import java.util.concurrent.TimeUnit;

@FunctionalInterface
public interface TimeUnitConverter {
    long convert(long value, TimeUnit timeUnit);
}
