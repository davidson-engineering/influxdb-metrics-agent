#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-02-20
# ---------------------------------------------------------------------------
"""An agent to manage a data pipeline.
It facilitates the acquisition of data from a variety of sources, processes it
and then store it in a database"""
# ---------------------------------------------------------------------------

from buffered import Buffer
from metrics_processor import MetricsProcessor

from metrics_processor.pipeline import (
    JSONReader,
    Formatter,
    TimeLocalizer,
    FieldExpander,
    PropertyConstructor,
    TimePrecision,
    OutlierRemover,
    PropertyMapper,
    BinaryOperations,
)
from metrics_processor import load_config
from mqtt_node_network.client import MQTTClient
from mqtt_node_network.initialize import initialize


def test_processor():

    import time

    config = load_config("config/application.toml")

    processing_buffer = Buffer(maxlen=config["processor"]["input_buffer_length"])
    output_buffer = Buffer(maxlen=config["processor"]["output_buffer_length"])

    # Metrics Processor Configuration
    # ************************************************************************
    # Create a metrics processor for the data pipeline
    metrics_processor = MetricsProcessor(
        input_buffer=processing_buffer,
        output_buffer=output_buffer,
        pipelines=[
            JSONReader,
            PropertyConstructor,
            TimeLocalizer,
            TimePrecision,
            FieldExpander,
            Formatter,
            OutlierRemover,
            # BinaryOperations,
            PropertyMapper,
        ],
        config=config,
    )

    # MQTT Client Configuration
    # ************************************************************************

    mqtt_config = initialize(config="config/application.toml", secrets=".env")

    client = MQTTClient(
        name=mqtt_config["mqtt"]["client"]["name"],
        broker_config=mqtt_config["mqtt"]["broker"],
        buffer=metrics_processor.input_buffer,
        topic_structure=mqtt_config["mqtt"]["node_network"]["topic_structure"],
    ).connect()

    client.subscribe(
        topic=mqtt_config["mqtt"]["client"]["subscribe_topics"],
        qos=mqtt_config["mqtt"]["client"]["subscribe_qos"],
    )

    while True:
        time.sleep(1)
