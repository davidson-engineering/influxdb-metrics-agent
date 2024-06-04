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
    PropertyConstructor,
    TimeLocalizer,
    TimePrecision,
    FieldExpander,
    Formatter,
    PropertyConstructor,
    OutlierRemover,
    PropertyMapper,
)
from metrics_processor import load_config


def main():

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

    while True:
        time.sleep(1)


if __name__ == "__main__":

    main()
