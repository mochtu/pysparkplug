"""Module containing errors and functions to handle them"""

from typing import Optional

from paho.mqtt import client as paho_mqtt

from pysparkplug._enums import ErrorCode

__all__ = ["MQTTError"]


class MQTTError(Exception):
    """Error from MQTT client"""


def check_error_code(
    error_int: int, *, ignore_codes: Optional[set[ErrorCode]] = None
) -> None:
    """Validate error code"""
    if error_int > 0:
        error_code = ErrorCode(error_int)
        if ignore_codes is None or error_code not in ignore_codes:
            raise MQTTError(error_code)


def check_connack_code(reason_code: paho_mqtt.ReasonCode) -> None:
    """Validate connack code"""
    if reason_code != 0:
        raise ConnectionError(str(reason_code))
