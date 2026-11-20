"""Preset test cases exposed by the settlement test dispatcher."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Literal

Kind = Literal["audit", "refund"]
Expectation = Literal["success", "failure"]


@dataclass(frozen=True)
class CaseDefinition:
    preset_id: str
    name: str
    kind: Kind
    description: str
    expectation: Expectation
    touches: str

    def to_dict(self) -> dict:
        return asdict(self)


PRESETS: tuple[CaseDefinition, ...] = (
    CaseDefinition(
        preset_id="AUD-001",
        name="订单金额与支付流水核对",
        kind="audit",
        description="逐单核对支付流水的合计金额是否等于订单金额。",
        expectation="success",
        touches="只读查询 orders、ledger_entries",
    ),
    CaseDefinition(
        preset_id="AUD-002",
        name="退款流水不超过支付流水",
        kind="audit",
        description="按订单核对累计退款金额不超过累计支付金额。",
        expectation="success",
        touches="只读查询 ledger_entries",
    ),
    CaseDefinition(
        preset_id="AUD-003",
        name="订单状态与净额一致性",
        kind="audit",
        description="已退款订单净额必须为零，部分退款订单净额必须在区间内。",
        expectation="success",
        touches="只读查询 orders、ledger_entries",
    ),
    CaseDefinition(
        preset_id="AUD-004",
        name="退款申请与退款流水对账",
        kind="audit",
        description="每单已生效退款申请的金额合计必须等于退款流水合计。",
        expectation="success",
        touches="只读查询 refund_requests、ledger_entries",
    ),
    CaseDefinition(
        preset_id="REF-001",
        name="部分退款 ORD-1001",
        kind="refund",
        description="对 ORD-1001 退款 25.00 元，写入流水并把订单置为部分退款。",
        expectation="success",
        touches="写入 refund_requests、ledger_entries、orders",
    ),
    CaseDefinition(
        preset_id="REF-002",
        name="全额退款 ORD-1002",
        kind="refund",
        description="对 ORD-1002 全额退款 42.00 元，订单置为已退款。",
        expectation="success",
        touches="写入 refund_requests、ledger_entries、orders",
    ),
    CaseDefinition(
        preset_id="REF-003",
        name="重复退款应被拒绝",
        kind="refund",
        description="对已全额退款的 ORD-1003 再次退款 10.00 元，预期业务拒绝并失败退出。",
        expectation="failure",
        touches="事务内校验，不写入",
    ),
    CaseDefinition(
        preset_id="REF-004",
        name="超额退款应被拒绝",
        kind="refund",
        description="对 ORD-1005（19.99 元）退款 25.00 元，预期业务拒绝并失败退出。",
        expectation="failure",
        touches="事务内校验，不写入",
    ),
    CaseDefinition(
        preset_id="REF-005",
        name="写入后异常退出（校验恢复）",
        kind="refund",
        description="对 ORD-1005 退款 5.00 元并提交，随后故意异常退出，用于验证失败用例也会恢复数据库。",
        expectation="success",
        touches="写入并提交后异常退出",
    ),
)

PRESETS_BY_ID: dict[str, CaseDefinition] = {case.preset_id: case for case in PRESETS}
