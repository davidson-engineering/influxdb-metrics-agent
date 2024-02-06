#!/usr/bin/env python
# -*- coding: utf-8 -*-
# ----------------------------------------------------------------------------
# Created By  : Matthew Davidson
# Created Date: 2024-01-23
# ---------------------------------------------------------------------------
"""Some demonstrative code for using the metrics agent"""
# ---------------------------------------------------------------------------
import logging


def main():
    import time
    import random

    from metrics_agent import MetricsAgent, AggregateStatistics
    from metrics_agent.db_client import InfluxDatabaseClient

    logging.basicConfig(level=logging.DEBUG)

    # Create a client for the agent to write data to a database
    db_client = InfluxDatabaseClient(
        "config/influx.toml", local_tz="America/Vancouver", default_bucket="testing"
    )

    # create the agent and assign it the client and desired aggregator, as well as the desired interval for updating the database
    agent = MetricsAgent(
        update_interval=0.2,
        db_client=db_client,
        server_enabled_tcp=True,
        upload_stats_enabled=True,
        # post_processors=[AggregateStatistics()],
    )

    # Simulating metric collection
    # n = 1000
    # val_max = 1000
    # for _ in range(n):
    #     metric_value = random.randint(1, val_max)
    #     metrics_agent.add_metric(name="random data", value=metric_value)

    # Wait for the agent to finish sending all metrics to the database before ending the program
    while True:
        time.sleep(1)


if __name__ == "__main__":
    main()
