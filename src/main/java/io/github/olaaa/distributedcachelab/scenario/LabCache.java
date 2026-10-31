package io.github.olaaa.distributedcachelab.scenario;

/* Общий вход для сброса локальных кэшей и их счётчиков между тестами и через POST /lab/reset. */
public interface LabCache {

    void invalidateAll();
}
