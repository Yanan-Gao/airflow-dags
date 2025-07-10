from functools import wraps
from typing import Optional, Any, Callable, Union


def return_on_key_error(default_value: Optional[Any] = None) -> Callable:

    def decorator(func: Callable) -> Callable:

        @wraps(func)
        def wrapper(*args, **kwargs) -> Union[Optional[Any], Any]:
            try:
                return func(*args, **kwargs)
            except KeyError:
                return default_value

        return wrapper

    return decorator
