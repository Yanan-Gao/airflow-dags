from typing import Callable

SpendTestMetrics = dict[str, float]
MetricValidationFunction = Callable[[SpendTestMetrics], None]


class ModelValidationError(ValueError):
    pass


# Note: threshold typically is expected to be between 0 and 100


def is_greater_than_on_target_threshold(spend_test_metrics: dict[str, float], threshold: float) -> None:
    otp = spend_test_metrics["on_target"]
    if spend_test_metrics["on_target"] < threshold:
        raise ModelValidationError(f"On target percentage was {otp}, whis lower than the threshold {threshold}")


def is_abs_diff_of_under_and_above_target_less_than_threshold(spend_test_metrics: dict[str, float], threshold: float) -> None:
    abs_diff_of_under_and_above_target = abs(spend_test_metrics["under_target"] - spend_test_metrics["above_target"])
    if abs_diff_of_under_and_above_target > threshold:
        raise ModelValidationError(
            f"Absolute difference between under and above target percentage was {abs_diff_of_under_and_above_target}, whis higher than the threshold {threshold}"
        )
