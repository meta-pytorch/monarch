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

``process_exit_timeout``
    Timeout for waiting on process exit during shutdown.

    - **Type**: ``str`` (duration format)
    - **Default**: ``"10s"``
    - **Environment**: ``HYPERACTOR_PROCESS_EXIT_TIMEOUT``

``stop_actor_timeout``
    Timeout for gracefully stopping actors.

    - **Type**: ``str`` (duration format)
    - **Default**: ``"10s"``
    - **Environment**: ``HYPERACTOR_STOP_ACTOR_TIMEOUT``

``cleanup_timeout``
    Timeout for cleanup operations during shutdown.

    - **Type**: ``str`` (duration format)
    - **Default**: ``"3s"``
    - **Environment**: ``HYPERACTOR_CLEANUP_TIMEOUT``

``actor_spawn_max_idle``
    Maximum idle time between updates while spawning actors in a proc mesh.

    - **Type**: ``str`` (duration format)
    - **Default**: ``"30s"``
    - **Environment**: ``HYPERACTOR_MESH_ACTOR_SPAWN_MAX_IDLE``

``get_actor_state_max_idle``
    Maximum idle time for actor state queries.

    - **Type**: ``str`` (duration format)
    - **Default**: ``"1m"``
    - **Environment**: ``HYPERACTOR_MESH_GET_ACTOR_STATE_MAX_IDLE``

``supervision_watchdog_timeout``
    Liveness timeout for the actor-mesh supervision stream.

    - **Type**: ``str`` (duration format)
    - **Default**: ``"2m"``
    - **Environment**: ``HYPERACTOR_MESH_SUPERVISION_WATCHDOG_TIMEOUT``

    During actor-mesh supervision, the controller is expected to
    periodically publish on the subscription stream (including benign
    updates). If no supervision message is observed within this
    timeout, the controller is assumed to be unreachable and the mesh
    transitions to an unhealthy state.

    This timeout is a watchdog against indefinite silence rather than
    a message-delivery guarantee, and may conservatively treat a quiet
    but healthy controller as failed. Increase this value in
    environments with long startup times or extended periods of
    inactivity (e.g., opt mode with PAR extraction).

``proc_stop_max_idle``
    Maximum idle time between updates while stopping procs.

    - **Type**: ``str`` (duration format)
    - **Default**: ``"30s"``
    - **Environment**: ``HYPERACTOR_MESH_PROC_STOP_MAX_IDLE``

``get_proc_state_max_idle``
    Maximum idle time for proc state queries.

    - **Type**: ``str`` (duration format)
    - **Default**: ``"1m"``
    - **Environment**: ``HYPERACTOR_MESH_GET_PROC_STATE_MAX_IDLE``

``mesh_terminate_timeout``
    Timeout per child during graceful mesh termination.

    - **Type**: ``str`` (duration format)
    - **Default**: ``"10s"``
    - **Environment**: ``HYPERACTOR_MESH_TERMINATE_TIMEOUT``


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

``read_log_buffer``
    Buffer size for reading logs (in bytes).

    - **Type**: ``int``
    - **Default**: ``100``
    - **Environment**: ``HYPERACTOR_READ_LOG_BUFFER``

``force_file_log``
    Force file-based logging regardless of environment.

    - **Type**: ``bool``
    - **Default**: ``False``
    - **Environment**: ``HYPERACTOR_FORCE_FILE_LOG``

``prefix_with_rank``
    Prefix log lines with rank information.

    - **Type**: ``bool``
    - **Default**: ``True``
    - **Environment**: ``HYPERACTOR_PREFIX_WITH_RANK``


Message Handling
----------------

``message_ack_time_interval``
    Time interval for message acknowledgments.

    - **Type**: ``str`` (duration format)
    - **Default**: ``"500ms"``
    - **Environment**: ``HYPERACTOR_MESSAGE_ACK_TIME_INTERVAL``

``message_ack_every_n_messages``
    Acknowledge every N messages.

    - **Type**: ``int``
    - **Default**: ``1000``
    - **Environment**: ``HYPERACTOR_MESSAGE_ACK_EVERY_N_MESSAGES``

``message_ttl_default``
    Default message time-to-live (number of hops).

    - **Type**: ``int``
    - **Default**: ``64``
    - **Environment**: ``HYPERACTOR_MESSAGE_TTL_DEFAULT``

``split_max_buffer_size``
    Maximum buffer size for message splitting (number of fragments).

    - **Type**: ``int``
    - **Default**: ``5``
    - **Environment**: ``HYPERACTOR_SPLIT_MAX_BUFFER_SIZE``

``split_max_buffer_age``
    Maximum age for split message buffers.

    - **Type**: ``str`` (duration format)
    - **Default**: ``"50ms"``
    - **Environment**: ``HYPERACTOR_SPLIT_MAX_BUFFER_AGE``

``channel_net_rx_buffer_full_check_interval``
    Network receive buffer check interval.

    - **Type**: ``str`` (duration format)
    - **Default**: ``"5s"``
    - **Environment**: ``HYPERACTOR_CHANNEL_NET_RX_BUFFER_FULL_CHECK_INTERVAL``

``message_latency_sampling_rate``
    Sampling rate for message latency tracking (0.0 to 1.0).

    - **Type**: ``float``
    - **Default**: ``0.01``
    - **Environment**: ``HYPERACTOR_MESSAGE_LATENCY_SAMPLING_RATE``

    A value of ``0.01`` means 1% of messages are sampled. Use ``1.0`` for
    100% sampling (all messages) or ``0.0`` to disable sampling.

``enable_dest_actor_reordering_buffer``
    Enable reordering buffer in dest actor.

    - **Type**: ``bool``
    - **Default**: ``False``
    - **Environment**: ``HYPERACTOR_ENABLE_DEST_ACTOR_REORDERING_BUFFER``


Message Encoding
----------------

``default_encoding``
    Default message encoding format.

    - **Type**: ``Encoding`` enum
    - **Default**: ``Encoding.Multipart``
    - **Environment**: ``HYPERACTOR_DEFAULT_ENCODING`` (accepts ``"bincode"``, ``"serde_json"``, or ``"serde_multipart"``)

    Supported values:

    - ``Encoding.Bincode`` - Bincode serialization (compact binary format via the ``bincode`` crate)
    - ``Encoding.Json`` - JSON serialization (via ``serde_json``)
    - ``Encoding.Multipart`` - Zero-copy multipart encoding that separates large binary fields from the message body, enabling efficient transmission via vectored I/O (default)

    Example usage::

        from monarch.config import Encoding, configure
        configure(default_encoding=Encoding.Bincode)


Mesh Bootstrap
--------------

``mesh_bootstrap_enable_pdeathsig``
    Enable parent-death signal for spawned processes.

    - **Type**: ``bool``
    - **Default**: ``True``
    - **Environment**: ``HYPERACTOR_MESH_BOOTSTRAP_ENABLE_PDEATHSIG``

    When ``True``, child processes receive SIGTERM if their parent dies,
    preventing orphaned processes.

``mesh_terminate_concurrency``
    Maximum concurrent terminations during mesh shutdown.

    - **Type**: ``int``
    - **Default**: ``16``
    - **Environment**: ``HYPERACTOR_MESH_TERMINATE_CONCURRENCY``


Runtime and Buffering
----------------------

``shared_asyncio_runtime``
    Share asyncio runtime across actors.

    - **Type**: ``bool``
    - **Default**: ``False``
    - **Environment**: ``MONARCH_HYPERACTOR_SHARED_ASYNCIO_RUNTIME``

``small_write_threshold``
    Threshold below which writes are copied (in bytes).

    - **Type**: ``int``
    - **Default**: ``256``
    - **Environment**: ``MONARCH_HYPERACTOR_SMALL_WRITE_THRESHOLD``

    Writes smaller than this threshold are copied into a contiguous buffer.
    Writes at or above this size are stored as zero-copy references.


Actor Configuration
-------------------

``actor_queue_dispatch``
    Enable queue-based dispatch for actor message handling.

    - **Type**: ``bool``
    - **Default**: ``False``
    - **Environment**: ``HYPERACTOR_ACTOR_QUEUE_DISPATCH``

    When ``True``, actor messages are dispatched through a queue rather than
    directly. This can improve throughput in high-message-volume scenarios.


Mesh Configuration
------------------

``max_cast_dimension_size``
    Maximum dimension size for cast operations.

    - **Type**: ``int``
    - **Default**: ``usize::MAX`` (platform-dependent)
    - **Environment**: ``HYPERACTOR_MESH_MAX_CAST_DIMENSION_SIZE``


Remote Allocation
-----------------

``remote_allocator_heartbeat_interval``
    Heartbeat interval for remote allocator.

    - **Type**: ``str`` (duration format)
    - **Default**: ``"5m"``
    - **Environment**: ``HYPERACTOR_REMOTE_ALLOCATOR_HEARTBEAT_INTERVAL``


Validation and Error Handling
-----------------------------

``configure`` and ``configured`` validate input immediately:

* Unknown keys raise ``ValueError``.
* Type mismatches raise ``TypeError`` (for example, passing a string instead
  of ``ChannelTransport`` for ``default_transport``, a non-bool to logging
  flags, or an integer instead of a string for duration parameters).
* Invalid values raise ``TypeError`` (for example, invalid encoding names,
  invalid port ranges, or malformed duration strings).
* Duration strings must follow
  `humantime <https://docs.rs/humantime/latest/humantime/>`_ syntax;
  invalid strings trigger ``TypeError`` with a message that highlights the
  bad value.

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
