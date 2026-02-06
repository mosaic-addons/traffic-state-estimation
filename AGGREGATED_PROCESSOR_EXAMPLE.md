# AggregatedSpatioTemporalProcessor Configuration Example

## Overview

The `AggregatedSpatioTemporalProcessor` aggregates traffic metrics over configurable time intervals (e.g., 15 minutes) for each connection. Unlike `SpatioTemporalProcessor`, which stores metrics for every individual traversal, this processor collects metrics in memory and writes aggregated averages (temporal mean speed, spatial mean speed, and sample count) at interval boundaries.

## Key Features

- **Time-based Aggregation**: Metrics are grouped into configurable time intervals (default: 15 minutes)
- **Processing Delay**: Configurable delay to handle late-arriving data (default: 5 minutes)
- **Automatic Flushing**: Completed intervals are automatically written to storage when new data arrives
- **Shutdown Safety**: All remaining data is flushed when the simulation ends

## Configuration Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `aggregationInterval` | Time (ns) | 15 minutes | Time window for aggregation |
| `processingDelay` | Time (ns) | 5 minutes | Delay before flushing to allow late data |
| `spatialMeanSpeedChunkSize` | Distance (m) | 15 meters | Chunk size for spatial mean speed calculation |

## Usage Example

### TseServerApp.json Configuration

```json
{
  "traversalBasedProcessors": [
    {
      "type": "SpatioTemporalProcessor",
      "spatialMeanSpeedChunkSize": "15 m"
    },
    {
      "type": "AggregatedSpatioTemporalProcessor",
      "aggregationInterval": "15 min",
      "processingDelay": "5 min",
      "spatialMeanSpeedChunkSize": "15 m"
    }
  ],
  "timeBasedProcessors": [
    {
      "type": "ThresholdProcessor",
      "triggerInterval": "30 min"
    }
  ],
  "parquetOutputPath": "/path/to/output"
}
```

### Using Only Aggregated Metrics

If you only want aggregated metrics (not individual traversal metrics), you can exclude `SpatioTemporalProcessor`:

```json
{
  "traversalBasedProcessors": [
    {
      "type": "AggregatedSpatioTemporalProcessor",
      "aggregationInterval": "15 min",
      "processingDelay": "5 min"
    }
  ],
  "timeBasedProcessors": [
    {
      "type": "ThresholdProcessor",
      "triggerInterval": "30 min"
    }
  ],
  "parquetOutputPath": "/path/to/output"
}
```

### Different Time Intervals

For shorter intervals (e.g., 5 minutes with 2-minute delay):

```json
{
  "traversalBasedProcessors": [
    {
      "type": "AggregatedSpatioTemporalProcessor",
      "aggregationInterval": "5 min",
      "processingDelay": "2 min"
    }
  ]
}
```

For longer intervals (e.g., 1 hour with 10-minute delay):

```json
{
  "traversalBasedProcessors": [
    {
      "type": "AggregatedSpatioTemporalProcessor",
      "aggregationInterval": "1 h",
      "processingDelay": "10 min"
    }
  ]
}
```

## Output

### SQLite Output

Aggregated metrics are stored in the `aggregated_traversal_metrics` table:

| Column | Type | Description |
|--------|------|-------------|
| `connectionID` | TEXT | Connection identifier |
| `intervalStart` | INTEGER | Start timestamp of interval (ns) |
| `intervalEnd` | INTEGER | End timestamp of interval (ns) |
| `temporalMeanSpeed` | REAL | Average temporal mean speed (m/s) |
| `spatialMeanSpeed` | REAL | Average spatial mean speed (m/s) |
| `naiveMeanSpeed` | REAL | Average naive mean speed (m/s) |
| `speedPerformanceIndex` | REAL | Average speed performance index |
| `sampleCount` | INTEGER | Number of traversals in interval |
| `timeOfInsertionUTC` | DATETIME | UTC timestamp of insertion |

### Parquet Output

Aggregated metrics are written to `aggregated_metrics.parquet` with the schema:

```json
{
  "type": "record",
  "name": "AggregatedMetric",
  "fields": [
    {"name": "connectionID", "type": "string"},
    {"name": "intervalStart", "type": "long"},
    {"name": "intervalEnd", "type": "long"},
    {"name": "avgTemporalMeanSpeed", "type": "double"},
    {"name": "avgSpatialMeanSpeed", "type": "double"},
    {"name": "avgNaiveMeanSpeed", "type": "double"},
    {"name": "avgSpeedPerformanceIndex", "type": "double"},
    {"name": "sampleCount", "type": "int"}
  ]
}
```

## How It Works

1. **Data Collection**: As vehicles traverse connections, the processor calculates temporal and spatial mean speeds (same calculation as `SpatioTemporalProcessor`)

2. **Interval Assignment**: Each traversal is assigned to a time interval based on its timestamp:
   ```
   intervalStart = (timestamp / aggregationInterval) * aggregationInterval
   ```

3. **Buffering**: Metrics are accumulated in memory for each (connection, interval) pair

4. **Flushing**: When a new traversal arrives with timestamp > (maxSeenTimestamp - processingDelay), all intervals older than this threshold are flushed to storage

5. **Shutdown**: Any remaining buffered data is written when the simulation ends

## Example Scenario

With `aggregationInterval = 15 min` and `processingDelay = 5 min`:

- Interval 1: 00:00 - 00:15
- Interval 2: 00:15 - 00:30
- Interval 3: 00:30 - 00:45

When a traversal arrives at timestamp 00:25:
- It gets added to Interval 2 (00:15 - 00:30)
- Interval 1 is flushed (because 00:25 - 5 min = 00:20, which is past 00:15)

## Benefits

- **Reduced Storage**: One aggregated record per connection per interval instead of one record per traversal
- **Better for Analysis**: Pre-computed averages make time-series analysis easier
- **Handles Late Data**: Processing delay allows late-arriving data to be included in the correct interval
- **Memory Efficient**: Only active intervals are kept in memory; old intervals are flushed automatically

## Compatibility

- Works with both SQLite (`FcdDatabaseHelper`) and Parquet (`FcdParquetStorage`) storage backends
- Can be used alongside `SpatioTemporalProcessor` to have both detailed and aggregated metrics
- Compatible with all existing processors (`ThresholdProcessor`, `FcdWriterProcessor`, etc.)
