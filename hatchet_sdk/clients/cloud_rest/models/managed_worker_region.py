# coding: utf-8

"""
    Hatchet API

    The Hatchet API

    The version of the OpenAPI document: 1.0.0
    Generated by OpenAPI Generator (https://openapi-generator.tech)

    Do not edit the class manually.
"""  # noqa: E501


from __future__ import annotations

import json
from enum import Enum

from typing_extensions import Self


class ManagedWorkerRegion(str, Enum):
    """
    ManagedWorkerRegion
    """

    """
    allowed enum values
    """
    AMS = "ams"
    ARN = "arn"
    ATL = "atl"
    BOG = "bog"
    BOS = "bos"
    CDG = "cdg"
    DEN = "den"
    DFW = "dfw"
    EWR = "ewr"
    EZE = "eze"
    GDL = "gdl"
    GIG = "gig"
    GRU = "gru"
    HKG = "hkg"
    IAD = "iad"
    JNB = "jnb"
    LAX = "lax"
    LHR = "lhr"
    MAD = "mad"
    MIA = "mia"
    NRT = "nrt"
    ORD = "ord"
    OTP = "otp"
    PHX = "phx"
    QRO = "qro"
    SCL = "scl"
    SEA = "sea"
    SIN = "sin"
    SJC = "sjc"
    SYD = "syd"
    WAW = "waw"
    YUL = "yul"
    YYZ = "yyz"

    @classmethod
    def from_json(cls, json_str: str) -> Self:
        """Create an instance of ManagedWorkerRegion from a JSON string"""
        return cls(json.loads(json_str))
