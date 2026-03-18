from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from datetime import date
from typing import Dict, List, Optional


class Rotation(Enum):
    FRI_MON = "Fri→Mon"
    MON_TUE = "Mon→Tue"
    TUE_WED = "Tue→Wed"
    WED_THU = "Wed→Thu"
    THU_FRI = "Thu→Fri"


@dataclass
class Quote:
    bid: float
    ask: float


@dataclass
class DividendInfo:
    ticker: str
    ex_date: date
    pay_date: Optional[date]
    amount: float


@dataclass
class Config:
    min_yield: float
    max_spread_bps: int
    max_spread_pct_div: float
    tier: Dict[str, str]
    rotation_map: Dict[str, Rotation]


@dataclass
class Signal:
    ticker: str
    rotation: Rotation
    ex_date: date
    pay_date: Optional[date]
    dividend: float
    mid_price: float
    yield_pct: float
    spread_bps: float
    spread_pct_of_div: float
    tier: str
    action: str
    reasons: List[str]


def mid_price(q: Quote) -> float:
    return (q.bid + q.ask) / 2.0


def spread_bps(q: Quote) -> float:
    m = mid_price(q)
    if m <= 0:
        return float("inf")
    return (q.ask - q.bid) / m * 10_000.0


def spread_pct_of_div(q: Quote, div_amount: float) -> float:
    if div_amount <= 0:
        return float("inf")
    return (q.ask - q.bid) / div_amount


def qualifies_today_for_rotation(today: date, div: DividendInfo, rotation: Rotation) -> bool:
    weekday = today.weekday()
    ex_weekday = div.ex_date.weekday()

    if rotation == Rotation.FRI_MON:
        return weekday == 4 and ex_weekday == 0

    if rotation == Rotation.MON_TUE:
        return weekday == 0 and ex_weekday == 1

    if rotation == Rotation.TUE_WED:
        return weekday == 1 and ex_weekday == 2

    if rotation == Rotation.WED_THU:
        return weekday == 2 and ex_weekday == 3

    if rotation == Rotation.THU_FRI:
        return weekday == 3 and ex_weekday == 4

    return False


def generate_signals(today, quotes, dividends, config):
    signals = []

    for ticker, div in dividends.items():
        q = quotes.get(ticker)
        if q is None:
            continue

        rotation = config.rotation_map.get(ticker)
        if rotation is None:
            continue

        if not qualifies_today_for_rotation(today, div, rotation):
            continue

        m = mid_price(q)
        y = div.amount / m if m > 0 else 0.0
        bps = spread_bps(q)
        spct = spread_pct_of_div(q, div.amount)

        reasons = []
        action = "BUY"

        if y < config.min_yield:
            reasons.append("yield too low")
            action = "SKIP"

        if bps > config.max_spread_bps:
            reasons.append("spread too wide")
            action = "SKIP"

        if spct > config.max_spread_pct_div:
            reasons.append("spread % of div too high")
            action = "SKIP"

        tier = config.tier.get(ticker, "B")

        signals.append(
            Signal(
                ticker=ticker,
                rotation=rotation,
                ex_date=div.ex_date,
                pay_date=div.pay_date,
                dividend=div.amount,
                mid_price=m,
                yield_pct=y * 100,
                spread_bps=bps,
                spread_pct_of_div=spct * 100,
                tier=tier,
                action=action,
                reasons=reasons if reasons else ["passed"],
            )
        )

    return signals
