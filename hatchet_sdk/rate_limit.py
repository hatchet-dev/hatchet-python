from dataclasses import dataclass
from typing import Union

from celpy import CELEvalError, Environment

from hatchet_sdk.contracts.workflows_pb2 import CreateStepRateLimit


def validate_cel_expression(expr: str) -> bool:
    env = Environment()
    try:
        env.compile(expr)
        return True
    except CELEvalError:
        return False


@dataclass
class RateLimit:
    key: Union[str, None] = None
    static_key: Union[str, None] = None
    dynamic_key: Union[str, None] = None
    units: Union[int, str] = 1
    limit: Union[int, str, None] = None

    _req: CreateStepRateLimit = None

    def __post_init__(self):
        # juggle the key and key_expr fields
        key = self.static_key
        key_expression = self.dynamic_key

        if self.key is not None:
            print(
                "key is deprecated and will be removed in a future release, please use static_key instead"
            )
            key = self.key

        if key_expression is not None:
            if key is not None:
                raise ValueError("Cannot have both static key and dynamic key set")

            key = key_expression
            if not validate_cel_expression(key_expression):
                raise ValueError(f"Invalid CEL expression: {key_expression}")

        # juggle the units and units_expr fields
        units = None
        units_expression = None
        if isinstance(self.units, int):
            units = self.units
        else:
            if not validate_cel_expression(self.units):
                raise ValueError(f"Invalid CEL expression: {self.units}")
            units_expression = self.units

        # juggle the limit and limit_expr fields
        limit_expression = None

        if self.limit:
            if isinstance(self.limit, int):
                limit_expression = f"{self.limit}"
            else:
                if not validate_cel_expression(self.limit):
                    raise ValueError(f"Invalid CEL expression: {self.limit}")
                limit_expression = self.limit

        if key_expression is not None and limit_expression is None:
            raise ValueError("CEL based keys requires limit to be set")

        self._req = CreateStepRateLimit(
            key=key,
            key_expr=key_expression,
            units=units,
            units_expr=units_expression,
            limit_values_expr=limit_expression,
        )


class RateLimitDuration:
    SECOND = "SECOND"
    MINUTE = "MINUTE"
    HOUR = "HOUR"
    DAY = "DAY"
    WEEK = "WEEK"
    MONTH = "MONTH"
    YEAR = "YEAR"
