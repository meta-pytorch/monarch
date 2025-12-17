monarch.config
==============

.. currentmodule:: monarch.config

The ``monarch.config`` module provides utilities for managing Monarch's
runtime configuration.

Configuration values can be set programmatically via :func:`configure`
or :func:`configured`, or through environment variables
(``HYPERACTOR_*``, ``MONARCH_*``). Programmatic configuration takes
precedence over environment variables and defaults.

Configuration API
=================

``monarch.config`` exposes a small, process-wide API. All helpers talk to
the same layered configuration store, so changes are immediately visible to
every thread in the process.

``configure``
    Apply overrides to the Runtime layer. Values are validated eagerly; a
    ``ValueError`` is raised for unknown keys and ``TypeError`` for wrong
    types. ``configure`` is additive, so you typically pair it with
    :func:`clear_runtime_config` in long-running processes.

``configured``
    Context manager sugar that snapshots the current Runtime layer,
    applies overrides, yields the merged config, then restores the snapshot.
    Because the Runtime layer is global, the overrides apply to every thread
    until the context exits. This makes ``configured`` ideal for tests or
    short-lived blocks where you can guarantee single-threaded execution.

``get_global_config``
    Return the fully merged configuration (defaults + environment + file +
    runtime). Useful for introspection or for passing a frozen view to other
    components.

``get_runtime_config``
    Return only the currently active Runtime layer. This is what ``configure``
    manipulates and what ``configured`` snapshots.

``clear_runtime_config``
    Reset the Runtime layer to an empty mapping. Environment and file values
    remain untouched.

.. autofunction:: configure

.. autofunction:: configured

.. autofunction:: get_global_config

.. autofunction:: get_runtime_config

.. autofunction:: clear_runtime_config


Configuration Keys
==================

The following configuration keys are available for use with
:func:`configure` and :func:`configured`:

Performance and Transport
--------------------------

``codec_max_frame_length``
    Maximum frame length for message codec (in bytes).

    - **Type**: ``int``
    - **Default**: ``10 * 1024 * 1024 * 1024`` (10 GiB)
    - **Environment**: ``HYPERACTOR_CODEC_MAX_FRAME_LENGTH``

    Controls the maximum size of serialized messages. Exceeding this limit
    will cause supervision errors.

    .. code-block:: python

        from monarch.config import configured

        # Allow larger messages for bulk data transfer
        oneHundredGiB = 100 * 1024 * 1024 * 1024
        with configured(codec_max_frame_length=oneHundredGiB):
            # Send large chunks
            result = actor.process_chunks.call_one(large_data).get()

``default_transport``
    Default channel transport mechanism for inter-actor communication.

    - **Type**: ``ChannelTransport`` enum
    - **Default**: ``ChannelTransport.Unix``
    - **Environment**: ``HYPERACTOR_DEFAULT_TRANSPORT``

    Available transports:

    - ``ChannelTransport.Unix`` - Unix domain sockets (local only)
    - ``ChannelTransport.TcpWithLocalhost`` - TCP over localhost
    - ``ChannelTransport.TcpWithHostname`` - TCP with hostname resolution
    - ``ChannelTransport.MetaTlsWithHostname`` - Meta TLS (Meta internal only)

    .. code-block:: python

        from monarch._rust_bindings.monarch_hyperactor.channel import (
            ChannelTransport,
        )
        from monarch.config import configured

        with configured(default_transport=ChannelTransport.TcpWithLocalhost):
            # Actors will communicate via TCP
            mesh = this_host().spawn_procs(per_host={"workers": 4})


Timeouts
--------

``message_delivery_timeout``
    Maximum time to wait for message delivery before timing out.

    - **Type**: ``str`` (duration format, e.g., ``"30s"``, ``"5m"``)
    - **Default**: ``"30s"``
    - **Environment**: ``HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT``

    Uses `humantime <https://docs.rs/humantime/latest/humantime/>`_ format.
    Examples: ``"30s"``, ``"5m"``, ``"1h 30m"``.

    .. code-block:: python

        from monarch.config import configured

        # Increase timeout for slow operations
        with configured(message_delivery_timeout="5m"):
            result = slow_actor.heavy_computation.call_one().get()

``host_spawn_ready_timeout``
    Maximum time to wait for spawned hosts to become ready.

    - **Type**: ``str`` (duration format)
    - **Default**: ``"30s"``
    - **Environment**: ``HYPERACTOR_HOST_SPAWN_READY_TIMEOUT``

    .. code-block:: python

        from monarch.config import configured

        # Allow more time for remote host allocation
        with configured(host_spawn_ready_timeout="5m"):
            hosts = HostMesh.allocate(...)

``mesh_proc_spawn_max_idle``
    Maximum idle time between status updates while spawning processes in a
    mesh.

    - **Type**: ``str`` (duration format)
    - **Default**: ``"30s"``
    - **Environment**: ``HYPERACTOR_MESH_PROC_SPAWN_MAX_IDLE``

    During proc mesh spawning, each process being created sends status
    updates to the controller. If no update arrives within this timeout, the
    spawn operation fails. This prevents hung or stuck process creation from
    waiting indefinitely.


Logging
-------

``enable_log_forwarding``
    Enable forwarding child process stdout/stderr over the mesh log channel.

    - **Type**: ``bool``
    - **Default**: ``False``
    - **Environment**: ``HYPERACTOR_MESH_ENABLE_LOG_FORWARDING``

    When ``True``, child process output is forwarded to ``LogForwardActor``
    for centralized logging.
    When ``False``, child processes inherit parent stdio.

    .. code-block:: python

        from monarch.config import configured

        with configured(enable_log_forwarding=True):
            # Child process logs will be forwarded
            mesh = this_host().spawn_procs(per_host={"workers": 4})

``enable_file_capture``
    Enable capturing child process output to log files on disk.

    - **Type**: ``bool``
    - **Default**: ``False``
    - **Environment**: ``HYPERACTOR_MESH_ENABLE_FILE_CAPTURE``

    When ``True``, child process output is written to host-scoped log
    files. Can be combined with ``enable_log_forwarding`` for both
    streaming and persistent logs.

``tail_log_lines``
    Number of recent log lines to retain in memory per process.

    - **Type**: ``int``
    - **Default**: ``0``
    - **Environment**: ``HYPERACTOR_MESH_TAIL_LOG_LINES``

    Maintains a rotating in-memory buffer of the most recent log lines for
    debugging.
    Independent of file capture.

    .. code-block:: python

        from monarch.config import configured

        # Keep last 100 lines for debugging
        with configured(tail_log_lines=100):
            mesh = this_host().spawn_procs(per_host={"workers": 4})

Validation and Error Handling
-----------------------------

``configure`` and ``configured`` validate input immediately:

* Unknown keys raise ``ValueError``.
* Type mismatches raise ``TypeError`` (for example, passing a string instead
  of ``ChannelTransport`` for ``default_transport`` or a non-bool to logging
  flags).
* Duration strings must follow
  `humantime <https://docs.rs/humantime/latest/humantime/>`_ syntax;
  invalid strings or non-string values trigger ``TypeError`` with a message
  that highlights the bad value.

Normalization
~~~~~~~~~~~~~

Duration values are normalized when read from :func:`get_global_config`. For
instance, setting ``host_spawn_ready_timeout="300s"`` yields ``"5m"`` when you
read it back. This matches the behavior exercised in
``monarch/python/tests/test_config.py`` and helps keep logs and telemetry
consistent.


Examples
========

Basic Configuration
-------------------

.. code-block:: python

    from monarch.config import configure, get_global_config

    # Set configuration values
    configure(enable_log_forwarding=True, tail_log_lines=100)

    # Read current configuration
    config = get_global_config()
    print(config["enable_log_forwarding"])  # True
    print(config["tail_log_lines"])  # 100


Temporary Configuration (Testing)
----------------------------------

.. code-block:: python

    from monarch.config import configured

    def test_with_custom_config():
        # Configuration is scoped to this context
        with configured(
            enable_log_forwarding=True,
            message_delivery_timeout="1m"
        ) as config:
            # Config is active here
            assert config["enable_log_forwarding"] is True

        # Config is automatically restored after the context


Nested Overrides
----------------

.. code-block:: python

    from monarch.config import configured

    with configured(default_transport=ChannelTransport.TcpWithLocalhost):
        # Inner config overrides logging knobs only; default_transport
        # stays put.
        with configured(
            enable_log_forwarding=True,
            tail_log_lines=50,
        ) as config:
            assert (
                config["default_transport"]
                == ChannelTransport.TcpWithLocalhost
            )
            assert config["enable_log_forwarding"]

    # After both contexts exit the process is back to the previous settings.


Duration Formats
----------------

.. code-block:: python

    from monarch.config import configured

    # Various duration formats are supported
    with configured(
        message_delivery_timeout="90s",        # 1m 30s
        host_spawn_ready_timeout="5m",         # 5 minutes
        mesh_proc_spawn_max_idle="1h 30m",     # 1 hour 30 minutes
    ):
        # Timeouts are active
        pass


Environment Variable Override
------------------------------

Configuration can also be set via environment variables:

.. code-block:: bash

    # Set codec max frame length to 100 GiB
    export HYPERACTOR_CODEC_MAX_FRAME_LENGTH=107374182400

    # Enable log forwarding
    export HYPERACTOR_MESH_ENABLE_LOG_FORWARDING=true

    # Set message delivery timeout to 5 minutes
    export HYPERACTOR_MESSAGE_DELIVERY_TIMEOUT=5m

Environment variables are read during initialization and can be overridden
programmatically.


See Also
========

- :doc:`../generated/examples/getting_started` - Getting started guide
- :doc:`monarch.actor` - Actor API documentation
