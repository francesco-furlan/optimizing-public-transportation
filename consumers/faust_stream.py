"""Defines trends calculations for stations"""
import logging

import faust

logger = logging.getLogger(__name__)

# Faust will ingest records from Kafka in this format
class Station(faust.Record):
    stop_id: int
    direction_id: str
    stop_name: str
    station_name: str
    station_descriptive_name: str
    station_id: int
    order: int
    red: bool
    blue: bool
    green: bool


# Faust will produce records to Kafka in this format
class TransformedStation(faust.Record):
    station_id: int
    station_name: str
    order: int
    line: str

app = faust.App("stations-stream", broker = "kafka://kafka0:19092", store = "memory://")

topic = app.topic(
    "jdbc.stations",
    value_type = Station
)

out_topic = app.topic(
    "com.udacity.stations.faust",
    value_type = TransformedStation,
    partitions = 1
)

table = app.Table(
   "com.udacity.stations.faust",
   default = TransformedStation,
   partitions = 1,
   changelog_topic = out_topic,
)

def get_color(event):
    assert isinstance(event, Station), "Event is not of Station type"
    if event.red:
        return "red"
    elif event.green:
        return "green"
    elif event.blue:
        return "blue"
    else:
        logger.warning(f"No color found for station {event.station_id}")
        return None

@app.agent(topic)
async def station_event(events):
    async for event in events:
        transformed_station = TransformedStation(
            station_id = event.station_id,
            station_name = event.station_name,
            order = event.order,
            line = get_color(event)
        )
        table[event.station_id] = transformed_station

if __name__ == "__main__":
    app.main()
