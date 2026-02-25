# Copyright (c) Meta Platforms, Inc. and affiliates.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.

# pyre-unsafe
import logging
import os
import socket
import sys
from pathlib import Path

logger = logging.getLogger(__name__)


def _handle_unhandled_exception(*args):
    logger.error("Uncaught exception", exc_info=args)


_glog_level_to_abbr = {
    "DEBUG": "V",  # V is for VERBOSE in glog
    "INFO": "I",
    "WARNING": "W",
    "ERROR": "E",
    "CRITICAL": "C",
}


def fix_exception_lines(tb_lines):
    formatted_lines = []
    for line in tb_lines:
        # Replace the standard file and line format with the custom format
        if line.startswith("  File"):
            # Extract the filename and line number
            parts = line.split(",")
            file_info = parts[0].strip()[6:-1]  # Remove '  File "' and '"'
            line_info = parts[1].strip()[5:]  # Remove 'line '
            new_line = f"  File {file_info}:{line_info}"
            if len(parts) > 2:
                new_line += ", " + ",".join(parts[2:]).strip()
            formatted_lines.append(new_line)
        else:
            formatted_lines.append(line.strip())
    return formatted_lines


class _Formatter(logging.Formatter):
    def __init__(self, suffix):
        self.suffix = suffix

    def format(self, record):
        message = record.getMessage()
        asctime = self.formatTime(record, "%m%d %H:%M:%S")

        lines = message.strip().split("\n")
        if record.exc_info:
            exc_info = fix_exception_lines(
                self.formatException(record.exc_info).split("\n")
            )
            lines.extend(exc_info)
        if record.stack_info:
            stack_info = self.formatStack(record.stack_info)
            lines.extend(stack_info.strip().split("\n"))

        shortlevel = _glog_level_to_abbr.get(record.levelname, record.levelname[0])

        prefix = (
            f"{shortlevel}{asctime}.{int(record.msecs * 1000):06d} "
            f"{record.filename}:"
            f"{record.lineno}]{self.suffix}"
        )
        return "\n".join(f"{prefix} {line}" for line in lines)


def initialize_logging(process_name=None):
    log_folder = os.environ.get("TORCH_MONARCH_LOG_FOLDER")
    log_level = os.environ.get("TORCH_MONARCH_LOG_LEVEL", "INFO")
    suffix = "" if process_name is None else f" {process_name}:"
    handler = None
    if log_folder is not None:
        log_folder_path = Path(log_folder)
        log_folder_path.mkdir(parents=True, exist_ok=True)
        safe_process_name = (
            process_name.replace("/", "_") if process_name else "logfile.log"
        )
        log_file_name = f"{safe_process_name}.log"
        log_file_path = log_folder_path / log_file_name
        handler = logging.FileHandler(log_file_path)
    else:
        handler = logging.StreamHandler()
    handler.setFormatter(_Formatter(suffix))
    handler.setLevel(log_level)
    logging.root.setLevel(log_level)
    logging.root.addHandler(handler)
    sys.excepthook = _handle_unhandled_exception


def gethostname():
    """Get the hostname of the machine."""
    hostname = socket.gethostname()
    hostname = hostname.replace(".facebook.com", "")
    return hostname
