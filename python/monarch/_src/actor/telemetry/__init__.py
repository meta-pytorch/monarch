# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-strict


import logging
import warnings
from typing import Optional, Sequence

import opentelemetry.metrics as metrics  # @manual=fbsource//third-party/pypi/opentelemetry-api:opentelemetry-api
import opentelemetry.trace as trace  # @manual=fbsource//third-party/pypi/opentelemetry-api:opentelemetry-api
from monarch._rust_bindings.monarch_hyperactor.telemetry import (  # @manual=//monarch/monarch_extension:monarch_extension
    forward_to_tracing,
    PyCounter,
    PyHistogram,
    PyUpDownCounter,
)
from monarch._src.actor.telemetry.rust_span_tracing import RustTracerProvider
from opentelemetry.context import Context
from opentelemetry.metrics import CallbackT
from opentelemetry.util.types import Attributes


class TracingForwarder(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        # Try to add actor_id from the current context to the logging record
        try:
            from monarch._src.actor.actor_mesh import _context, context

            # Don't initialize the context if it hasn't been initialized yet.
            if _context.get(None) is not None:
                ctx = context()
                if ctx and ctx.actor_instance and ctx.actor_instance.actor_id:
                    # Add actor_id as an attribute to the logging record
                    setattr(record, "actor_id", str(ctx.actor_instance.actor_id))
        except Exception:
            # If we can't get the context or actor_id for any reason, just continue
            # without adding the actor_id field
            pass

        forward_to_tracing(record)


class Counter(metrics.Counter):
    inner: PyCounter

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.inner = PyCounter(name)

    def add(
        self,
        amount: int | float,
        attributes: Optional[Attributes] = None,
        context: Optional[Context] = None,
    ) -> None:
        rust_attributes = None
        if attributes:
            rust_attributes = {str(k): str(v) for k, v in attributes.items()}
        return self.inner.add(int(amount), attributes=rust_attributes)


class UpDownCounter(metrics.UpDownCounter):
    inner: PyUpDownCounter

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.inner = PyUpDownCounter(name)

    def add(
        self,
        amount: int | float,
        attributes: Optional[Attributes] = None,
        context: Optional[Context] = None,
    ) -> None:
        rust_attributes = None
        if attributes:
            rust_attributes = {str(k): str(v) for k, v in attributes.items()}
        self.inner.add(int(amount), attributes=rust_attributes)


class Histogram(metrics.Histogram):
    inner: PyHistogram

    def __init__(self, name: str) -> None:
        super().__init__(name)
        self.inner = PyHistogram(name)

    def record(
        self,
        amount: int | float,
        attributes: Optional[Attributes] = None,
        context: Optional[Context] = None,
    ) -> None:
        rust_attributes = None
        if attributes:
            rust_attributes = {str(k): str(v) for k, v in attributes.items()}
        self.inner.record(amount, attributes=rust_attributes)


class Meter(metrics.Meter):
    def create_counter(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> metrics.Counter:
        return Counter(name)

    def create_up_down_counter(
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> metrics.UpDownCounter:
        return UpDownCounter(name)

    def create_observable_counter(
        self,
        name: str,
        callbacks: Optional[Sequence[CallbackT]] = None,
        unit: str = "",
        description: str = "",
    ) -> metrics.ObservableCounter:
        raise NotImplementedError()

    def create_histogram(
        self,
        name: str,
        unit: str = "",
        description: str = "",
        *,
        explicit_bucket_boundaries_advisory: Optional[Sequence[float]] = None,
    ) -> metrics.Histogram:
        return Histogram(name)

    def create_gauge(  # type: ignore # pylint: disable=no-self-use
        self,
        name: str,
        unit: str = "",
        description: str = "",
    ) -> metrics._Gauge:  # pyright: ignore[reportReturnType]
        warnings.warn(
            "create_gauge() is not implemented and will be a no-op", stacklevel=2
        )
        raise NotImplementedError()

    def create_observable_gauge(
        self,
        name: str,
        callbacks: Optional[Sequence[CallbackT]] = None,
        unit: str = "",
        description: str = "",
    ) -> metrics.ObservableGauge:
        raise NotImplementedError()

    def create_observable_up_down_counter(
        self,
        name: str,
        callbacks: Optional[Sequence[CallbackT]] = None,
        unit: str = "",
        description: str = "",
    ) -> metrics.ObservableUpDownCounter:
        raise NotImplementedError()


class MeterProvider(metrics.MeterProvider):
    def get_meter(
        self,
        name: str,
        version: Optional[str] = None,
        schema_url: Optional[str] = None,
        attributes: Optional[Attributes] = None,
    ) -> metrics.Meter:
        return Meter(name, version, schema_url)


def get_monarch_tracer() -> trace.Tracer:
    """
    Creates and returns a Monarch python tracer that logs to the Rust telemetry system.

    Returns:
        Tracer: A configured OpenTelemetry tracer for Monarch.

    Usage:
        tracer = get_monarch_tracer()
        with tracer.start_as_current_span("span_name") as span:
            # code here
    """
    install()
    return trace.get_tracer("monarch.python.tracer")


_INSTALLED = False

METER: metrics.Meter = metrics.get_meter("monarch")


def install() -> None:
    global _INSTALLED
    if _INSTALLED:
        return

    provider = RustTracerProvider()
    trace.set_tracer_provider(provider)
    metrics.set_meter_provider(MeterProvider())

    global METER
    METER = metrics.get_meter("monarch")
    _INSTALLED = True
