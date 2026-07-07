"""Alert notifier adapter: post fired alerts to a Slack-compatible webhook, or log to console
when none is configured."""

from __future__ import annotations

import logging

import httpx

from log_aggregator.config import Settings

log = logging.getLogger("alerting")


def make_notifier(settings: Settings):
    webhook = settings.alert_webhook

    async def notify(alert: dict) -> None:
        text = f":rotating_light: {alert['message']} (tenant={alert['tenant']})"
        if webhook:
            try:
                async with httpx.AsyncClient(timeout=5) as client:
                    await client.post(webhook, json={"text": text})
            except Exception as exc:  # noqa: BLE001 — a flaky webhook must not kill the worker
                log.warning("webhook post failed: %s", exc)
        else:
            log.warning("ALERT %s", text)

    return notify
