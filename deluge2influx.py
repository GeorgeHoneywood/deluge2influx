import logging
import os
import time
from datetime import datetime

from deluge_client import DelugeRPCClient, FailedToReconnectException
from influxdb import InfluxDBClient


class DelugeStatCollector:
    """
    Collect stats from Deluge and pass them into InfluxDB
    """

    # see https://www.rasterbar.com/products/libtorrent/reference-Torrent_Status.html
    # however all_time_upload not present in python
    retrieve_keys = []

    def __init__(
        self,
        host="localhost",
        port=58846,
        username="user",
        password="password",
        influxdb_host="localhost",
        retrieve_keys=[],
    ):
        if not retrieve_keys:
            self.retrieve_keys = [
                "name",
                "ratio",
                "progress",
                "all_time_download",
                "total_uploaded",
                "num_peers",
                "num_seeds",
            ]
        else:
            self.retrieve_keys = retrieve_keys

        logging.info(f"Connecting to clients!")

        self.deluge_rpc_client = DelugeRPCClient(
            host,
            port,
            username,
            password,
            automatic_reconnect=True,
        )

        # The client has to be online when you start the process,
        # otherwise you must handle that yourself.
        self.deluge_rpc_client.connect()

        self.influxdb_client = InfluxDBClient(host=influxdb_host, port=8086)
        self.influxdb_client.switch_database("deluge")

        logging.info(f"Started logging to InfluxDB!")
        self.timer()

    def call_retry(self, method, *args, **kwargs):
        # We will only try the command 10 times
        for _ in range(10):
            try:
                return self.deluge_rpc_client.call(method, *args, **kwargs)
            except FailedToReconnectException:
                # 5 second delay between calls
                time.sleep(15)

    def timer(self):
        while True:
            self.get_stats()
            time.sleep(10)

    def get_stats(self):
        self.write_stats(
            self.call_retry("core.get_torrents_status", {}, self.retrieve_keys)
        )

    def write_stats(self, torrent_stats):
        points = []

        for hash, stats in torrent_stats.items():
            fields = {}

            for stat_name, stat_value in stats.items():
                fields[stat_name.decode()] = stat_value

            points.append(
                {
                    "measurement": "torrent",
                    "time": datetime.utcnow(),
                    "fields": fields,
                    "tags": {"hash": hash.decode(), "name": stats[b"name"]},
                }
            )

        logging.info(f"Collected {len(points)} data points!")

        self.influxdb_client.write_points(points)



if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)s:%(levelname)s - %(message)s', datefmt='%H:%M:%S', level=logging.INFO)

    deluge_stat_collector = DelugeStatCollector(
        host="192.168.1.20",
        username="HoneyFox",
        password=os.getenv("DELUGED_PW"),
        influxdb_host="honeyfox-influxdb",
    )
