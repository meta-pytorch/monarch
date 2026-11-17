# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

import logging

import pytest
from monarch._src.actor import telemetry


@pytest.fixture
def forwarded(monkeypatch):
    records = []

    def forward(record):
        records.append(record.getMessage())

    monkeypatch.setattr(telemetry, "forward_to_tracing", forward)
    monkeypatch.setattr(telemetry, "_forwarding_installed", False)
    factory = logging.getLogRecordFactory()
    telemetry.forward_logs_to_tracing()
    yield records
    logging.setLogRecordFactory(factory)


@pytest.mark.parametrize("prefix", ["", "[actor=<root>] "])
def test_every_record_is_forwarded_once(forwarded, prefix):
    logger = logging.getLogger("test_log_forwarding")
    handler = telemetry.TracingForwarder()

    def add_prefix(record):
        record.msg = f"{prefix}{record.msg}"
        return True

    handler.addFilter(add_prefix)
    logger.addHandler(handler)
    try:
        logger.warning("plain %s", 1)
        telemetry.log_with_tracing(logging.WARNING, "direct", logger=logger)
    finally:
        logger.removeHandler(handler)
    assert forwarded == ["plain 1", "direct"]


def test_placeholder_records_are_not_forwarded(forwarded):
    logging.makeLogRecord({"name": "remote", "msg": "received over a socket"})
    assert forwarded == []


def test_logging_while_forwarding_is_not_forwarded_again(monkeypatch, forwarded):
    logger = logging.getLogger("test_log_forwarding")

    def forward_and_log(record):
        forwarded.append(record.getMessage())
        logger.warning("nested")

    monkeypatch.setattr(telemetry, "forward_to_tracing", forward_and_log)
    logger.warning("outer")
    assert forwarded == ["outer"]


def test_installing_twice_forwards_once(forwarded):
    telemetry.forward_logs_to_tracing()
    logging.getLogger("test_log_forwarding").warning("once")
    assert forwarded == ["once"]
