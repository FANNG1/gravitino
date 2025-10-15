package org.apache.gravitino.updater.api;

// The stats provider to compute stats, the stats will be used to update Gravitino stats store or
// external systems.
public interface StatsComputer extends Computer {}
