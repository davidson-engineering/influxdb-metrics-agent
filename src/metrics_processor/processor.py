#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# Copyright © 2024 Davidson Engineering Ltd.
# ---------------------------------------------------------------------------
"""An agent for collecting, aggregating and sending metrics to a database"""
# ---------------------------------------------------------------------------

from collections import deque
import time
import threading
import logging
from typing import Union
from pathlib import Path

from prometheus_client import Gauge, Counter, start_http_server
from metrics_processor.exceptions import ConfigFileDoesNotExist

from buffered.buffer import Buffer

logger = logging.getLogger(__name__)

BUFFER_LENGTH = 65_536
STATS_UPLOAD_ENABLED = True
STATS_UPLOAD_INTERVAL_SECONDS = 60
BATCH_SIZE_SENDING = 5_000
BATCH_SIZE_PROCESSING = 1_000
UPDATE_INTERVAL_SECONDS = 10


def load_config(filepath: Union[str, Path]) -> dict:
    if isinstance(filepath, str):
        filepath = Path(filepath)

    if not Path(filepath).exists():
        raise FileNotFoundError(f"File not found: {filepath}")

    # if extension is .json
    if filepath.suffix == ".json":
        import json

        with open(filepath, "r") as file:
            return json.load(file)

    # if extension is .yaml
    if filepath.suffix == ".yaml":
        import yaml

        with open(filepath, "r") as file:
            return yaml.safe_load(file)
    # if extension is .toml
    if filepath.suffix == ".toml":
        try:
            import tomllib
        except ImportError:
            import tomli as tomllib

        with open(filepath, "rb") as file:
            return tomllib.load(file)

    # else load as binary
    with open(filepath, "rb") as file:
        return file.read()


def csv_to_metrics(csv_filepath):
    import pandas as pd
    import numpy as np

    df = pd.read_csv(csv_filepath)
    # Convert 'Time' column to integer
    df["time"] = df["time"].astype(int)

    # Convert 'nan' strings to actual NaN values
    df.replace("nan", np.nan, inplace=True)

    # Convert DataFrame to a list of dictionaries
    metrics = []
    for _, row in df.iterrows():
        metric = {"time": row["time"], "fields": row.drop("time").to_dict()}
        metrics.append(metric)
    # Convert DataFrame to a list of dictionaries
    return metrics


def shorten_data(data: str, max_length: int = 75) -> str:
    """Shorten data to a maximum length."""
    if not isinstance(data, str):
        data = str(data)
    data = data.strip()
    return data[:max_length] + "..." if len(data) > max_length else data


class MetricsProcessor:
    """

    An agent for collecting and processing metrics

    :param interval: The interval at which the agent will aggregate and send metrics to the database
    :param server: Whether to start a server to receive metrics from other agents
    :param client: The client to send metrics to
    :param aggregator: The aggregator to use to aggregate metrics
    :param autostart: Whether to start the aggregator thread automatically

    """

    # Initialize prometheus metrics
    buffer_occupancy = Gauge(
        "metricsprocessor_buffer_occupancy",
        "The occupancy of the buffer",
        ["agent", "buffer"],
    )
    metrics_processed = Counter(
        "metricsprocessor_metrics_processed",
        "The number of metrics processed",
        ["agent"],
    )

    def __init__(
        self,
        input_buffer: Union[list, deque, Buffer] = None,
        output_buffer: Union[list, deque, Buffer] = None,
        pipelines: Union[list, tuple] = None,
        autostart: bool = True,
        update_interval: float = None,
        config: Union[dict, str] = None,
    ):

        # Setup Agent
        # *************************************************************************
        # Parse configuration from file
        if isinstance(config, str):
            if not Path(config).exists():
                raise ConfigFileDoesNotExist
            config = load_config(config)

        # If no configuation specified, then set as blank dict so default values will be used
        self.config = config.get("processor", {})

        self.update_interval = update_interval or self.config.get(
            "update_interval", UPDATE_INTERVAL_SECONDS
        )

        input_buffer_length: int = self.config.get("input_buffer_length", BUFFER_LENGTH)
        output_buffer_length: int = self.config.get(
            "output_buffer_length", BUFFER_LENGTH
        )

        self.batch_size_processing = self.config.get(
            "batch_size", BATCH_SIZE_PROCESSING
        )

        # Set up the agent buffers
        if input_buffer is None:
            input_buffer = Buffer(maxlen=input_buffer_length)
        self.input_buffer: Union[list, deque, Buffer] = input_buffer
        if output_buffer is None:
            output_buffer = Buffer(maxlen=output_buffer_length)
        self.output_buffer: Union[list, deque, Buffer] = output_buffer

        self.pipelines = []
        # Instantiate pipelines if not already done so
        for pipeline in pipelines:
            if isinstance(pipeline, type):  # Check if it's a class type
                self.pipelines.append(pipeline())
            else:
                self.pipelines.append(pipeline)

        if autostart:
            self.start()

    def add_metric_to_queue(
        self, measurement: str, fields: dict, time: int = None, **kwargs
    ):
        metric = dict(measurement=measurement, fields=fields, time=time, **kwargs)
        self.input_buffer.append(metric)
        metric_str = shorten_data(f"{measurement}={fields}")
        logger.debug(f"Added metric to buffer: {metric_str}")

    def process_input_buffer(self):
        if self.input_buffer.not_empty():
            # dump buffer to list of metrics
            metrics = self.input_buffer.dump(self.batch_size_processing)
            for pipeline in self.pipelines:
                number_metrics_initial = len(metrics)
                metrics = pipeline.process(metrics)
                number_metrics_final = len(metrics)
                logger.info(
                    f"Processed {number_metrics_initial} metrics using {pipeline}. {number_metrics_final} metrics output"
                )
            self.output_buffer.extend(metrics)
            self.metrics_processed.labels("metrics_processor").inc(len(metrics))

    def passthrough(self):
        # If no post processors are defined, pass through the input buffer to the send buffer
        if self.input_buffer.not_empty():
            self.output_buffer.extend(
                self.input_buffer.dump(self.batch_size_processing)
            )

    def update_prometheus_metrics(self):
        # Update prometheus metrics
        self.buffer_occupancy.labels("metrics_processor", "input").set(
            self.input_buffer.size()
        )
        self.buffer_occupancy.labels("metrics_processor", "output").set(
            self.output_buffer.size()
        )

    # Thread management methods
    # *************************************************************************

    def run_processing(self):
        while True:
            if self.pipelines:
                self.process_input_buffer()
            else:
                self.passthrough()

            self.update_prometheus_metrics()

    def stop(self):
        self.processing_thread.join()
        logger.debug(f"Stopped processing metrics in thread {self.processing_thread}")

    def start_prometheus_server(self, port=8000):
        # Start Prometheus HTTP server
        start_http_server(port)
        logger.info(f"Prometheus server started on port {port}")

    def start(self):
        if self.config["prometheus"]["enable_prometheus_server"]:
            self.start_prometheus_server(
                port=self.config["prometheus"]["prometheus_port"]
            )
        self.processing_thread = threading.Thread(
            target=self.run_processing, daemon=True
        )
        self.processing_thread.start()
        logger.debug(f"Started processing metrics in thread {self.processing_thread}")
        return self

    # Buffer management methods
    # *************************************************************************

    def clear_input_buffer(self):
        self.input_buffer.clear()

    def clear_output_buffer(self):
        self.output_buffer.clear()

    def get_input_buffer_occupancy(self):
        return self.input_buffer.size()

    def get_output_buffer_occupancy(self):
        return self.output_buffer.size()

    def get_buffer_occupancy(self):
        return str(self.input_buffer), str(self.output_buffer)

    def run_until_buffer_empty(self):
        while self.input_buffer.not_empty():
            time.sleep(self.update_interval)
        logger.debug("Buffer is empty")

    def __repr__(self):
        return f"{self.__class__.__name__}({self.get_buffer_occupancy()})"

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.__del__()

    def __del__(self):
        try:
            # This method is called when the object is about to be destroyed
            self.stop()
        except AttributeError:
            pass
        logger.info(f"Metrics agent {self} destroyed")
