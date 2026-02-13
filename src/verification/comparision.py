import datetime
from typing import Any

import math
from numbers import Number

from collections.abc import Iterable
from collections import Counter

def make_hashable(item):
    if isinstance(item, list):
        return tuple(make_hashable(subitem) for subitem in item)
    elif isinstance(item, dict):
        return tuple(sorted((make_hashable(k), make_hashable(v)) for k, v in item.items()))
    elif isinstance(item, Iterable) and not isinstance(item, (str, bytes)):
        return tuple(make_hashable(subitem) for subitem in item)
    else:
        return item


def deep_equal(v1: Any, v2: Any, rel_tol=1e-6, abs_tol=1e-9) -> bool:
    if type(v1) != type(v2):
        return False

    if isinstance(v1, float) and isinstance(v2, float):
        return math.isclose(v1, v2, rel_tol=rel_tol, abs_tol=abs_tol)

    if isinstance(v1, (list, tuple)):
        if len(v1) != len(v2):
            return False
        return all(deep_equal(sub1, sub2, rel_tol, abs_tol) for sub1, sub2 in zip(v1, v2))

    if isinstance(v1, dict):
        if set(v1.keys()) != set(v2.keys()):
            return False
        return all(deep_equal(v1[k], v2[k], rel_tol, abs_tol) for k in v1.keys())

    if isinstance(v1, set):
        # 注意：set 中如果有浮点数，需要特殊处理
        if len(v1) != len(v2):
            return False
        # 转为列表后尝试匹配（因为 set 无序，且浮点不能精确哈希）
        unmatched = list(v2)
        for item1 in v1:
            matched = False
            for i, item2 in enumerate(unmatched):
                if deep_equal(item1, item2, rel_tol, abs_tol):
                    unmatched.pop(i)
                    matched = True
                    break
            if not matched:
                return False
        return True
    return v1 == v2


def tol_order_aware_compare(value1: list[tuple | None], value2: list[tuple | None],
                            rel_tol=1e-6, abs_tol=1e-9) -> bool:
    for item1, item2 in zip(value1, value2):
        if not tol_aware_recursive_compare(item1, item2, rel_tol, abs_tol):
            print(f"Mismatch found: {item1} vs {item2}")
            return False
    return True


def normalize_for_sorting(item: Any, rel_tol=1e-6, abs_tol=1e-9) -> Any:
    if isinstance(item, float):
        return item
    if isinstance(item, datetime.date) and not isinstance(item, datetime.datetime):
        return datetime.datetime.combine(item, datetime.time.min)
    if isinstance(item, list):
        normalized = sorted(normalize_for_sorting(x, rel_tol, abs_tol) for x in item)
        return tuple(normalized)
    if isinstance(item, tuple):
        return tuple(normalize_for_sorting(x, rel_tol, abs_tol) for x in item)
    if isinstance(item, dict):
        return tuple(
            (k, normalize_for_sorting(v, rel_tol, abs_tol))
            for k, v in sorted(item.items())
        )
    if isinstance(item, set):
        return tuple(sorted(normalize_for_sorting(x, rel_tol, abs_tol) for x in item))
    return item


def tol_aware_recursive_compare(item1: Any, item2: Any, rel_tol=1e-6, abs_tol=1e-9) -> bool:
    """Recursively compares two items, handling floats with tolerance."""
    if isinstance(item1, Number) and isinstance(item2, Number):
        return math.isclose(item1, item2, rel_tol=rel_tol, abs_tol=abs_tol)

    if isinstance(item1, (tuple, list)) and isinstance(item2, (tuple, list)):
        if len(item1) != len(item2):
            return False
        return all(
            tol_aware_recursive_compare(x, y, rel_tol, abs_tol) for x, y in zip(item1, item2)
        )
    if type(item1) != type(item2):
        return False
    return item1 == item2


def tol_order_unaware_compare(
        value1: list[tuple | None],
        value2: list[tuple | None],
        rel_tol=1e-6,
        abs_tol=1e-9
) -> bool:
    if len(value1) != len(value2):
        return False

    # Normalize and sort non-float items for comparison
    normalized1 = [normalize_for_sorting(item, rel_tol, abs_tol) for item in value1]
    normalized2 = [normalize_for_sorting(item, rel_tol, abs_tol) for item in value2]
    sorted1 = sorted(normalized1, key=lambda x: str(x))
    sorted2 = sorted(normalized2, key=lambda x: str(x))

    for item1, item2 in zip(sorted1, sorted2):
        if not tol_aware_recursive_compare(item1, item2, rel_tol, abs_tol):
            print(f"Mismatch found: {item1} vs {item2}")
            return False
    return True


def tol_order_unaware_compare_fallback(value1: list[tuple | None], value2: list[tuple | None],
                                       rel_tol=1e-6, abs_tol=1e-9) -> bool:
    if len(value1) != len(value2):
        return False

    unmatched = value2.copy()
    for item1 in value1:
        matched = False
        for i, item2 in enumerate(unmatched):
            if deep_equal(item1, item2, rel_tol=rel_tol, abs_tol=abs_tol):
                unmatched.pop(i)
                matched = True
                break
        if not matched:
            return False
    return True


def order_unaware_compare(value1: list[tuple | None], value2: list[tuple | None]):
    return Counter(make_hashable(item) for item in value1) == Counter(make_hashable(item) for item in value2)


def order_aware_compare(value1: list[tuple | None], value2: list[tuple | None]):
    return value1 == value2