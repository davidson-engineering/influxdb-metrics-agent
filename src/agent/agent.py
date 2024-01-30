#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# ---------------------------------------------------------------------------
"""An agent for collecting, aggregating and sending metrics to a database"""
# ---------------------------------------------------------------------------

import time
import threading
import logging
from datetime import datetime

from agent.aggregator import MetricsAggregatorStats
from agent.buffer import MetricsBuffer
from network_sync import MetricsServer, MetricTCPHandler

logger = logging.getLogger(__name__)


class DataFormatException(Exception):
    pass


class MetricsAgent:
    """

    An agent for collecting, aggregating and sending metrics to a database

    :param interval: The interval at which the agent will aggregate and send metrics to the database
    :param server: Whether to start a server to receive metrics from other agents
    :param client: The client to send metrics to
    :param aggregator: The aggregator to use to aggregate metrics
    :param autostart: Whether to start the aggregator thread automatically
    :param port: The port to start the server on
    :param host: The host to start the server on

    """

    def __init__(
        self,
        interval=10,
        server=None,
        client=None,
        aggregator=None,
        autostart=True,
        port=9000,
        host="localhost",
    ):
        self._metrics_buffer = MetricsBuffer()
        self._last_sent_time = time.time()
        self._lock = threading.Lock()  # To ensure thread safety

        if server is True:
            self.server: MetricsServer = MetricsServer((host, port), MetricTCPHandler)

            self.server_thread: threading.Thread = threading.Thread(
                target=self.server.start_server, daemon=True
            ).start()

            self.server_datafeed_thread: threading.Thread = threading.Thread(
                target=self.feed_data_from_server, daemon=True
            ).start()

        else:
            self.server = server

        self.interval = interval
        self.client = client
        self.aggregator = aggregator or MetricsAggregatorStats()

        if autostart:
            self.start_aggregator_thread()
            logger.info("Started aggregator thread")

    def add_metric(self, name, value, timestamp=None):
        with self._lock:
            self._metrics_buffer.add_metric(name, value, timestamp)
            logger.debug(f"Added metric to buffer: {name}={value}")

    def aggregate_and_send(self):
        with self._lock:
            if time.time() - self._last_sent_time >= self.interval:
                if self._metrics_buffer.not_empty():
                    # dump buffer to list of metrics
                    metrics = self._metrics_buffer.dump_buffer()
                    self._last_sent_time = time.time()
                    aggregated_metrics = self.aggregator.aggregate(metrics)
                    self.client.send(aggregated_metrics)

    def start_aggregator_thread(self):
        self.aggregator_thread = threading.Thread(
            target=self.run_aggregator, daemon=True
        ).start()
        logger.debug("Started aggregator thread")

    def run_aggregator(self):
        while True:
            self.aggregate_and_send()
            time.sleep(self.interval)  # Adjust sleep time as needed

    def stop_aggregator_thread(self):
        self.aggregator_thread.join()
        logger.debug("Stopped aggregator thread")

    def clear_metrics_buffer(self):
        with self._lock:
            self._metrics_buffer.clear_buffer()

    def get_metrics_buffer_size(self):
        return self._metrics_buffer.get_buffer_size()

    def run_until_buffer_empty(self):
        while self._metrics_buffer.not_empty():
            time.sleep(self.interval)
        logger.debug("Buffer is empty")

    def feed_data_from_server(self):
        # Check that data from buffer is in correct format for add_metric
        while True:
            logger.debug("Checking for data from server")
            if data_new := self.server.fetch_buffer():
                for data in data_new:
                    if not data[0]:
                        continue
                    name, value, timestamp = data
                    self.add_metric(
                        name=name,
                        value=float(value),
                        timestamp=datetime.fromtimestamp(float(timestamp)),
                    )
            else:
                logger.debug("No data from server, sleeping")
                time.sleep(1)

    def start(self):
        self.start_aggregator_thread()
        return self

    def __del__(self):
        try:
            # This method is called when the object is about to be destroyed
            self.stop_aggregator_thread()
            logger.debug("Stopped aggregator thread")
        except AttributeError:
            pass
        try:
            self.server.stop_server()
            logger.debug("Stopped server thread")
        except AttributeError:
            pass


def main():
    from agent.db_client import InfluxDatabaseClient

    client = InfluxDatabaseClient("config/influx.toml", local_tz="America/Vancouver")

    # Example usage
    metrics_agent = MetricsAgent(
        interval=1, client=client, aggregator=MetricsAggregatorStats()
    )

    n = 1000000
    # Simulating metric collection
    for _ in range(n):
        metrics_agent.add_metric(name="queries", value=True)

    # Wait for the agent to finish sending all metrics to the database before ending the program
    metrics_agent.run_until_buffer_empty()


if __name__ == "__main__":
    main()
