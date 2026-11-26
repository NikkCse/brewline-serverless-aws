"""Nightly (11 PM America/Toronto) summary of today's orders, emailed to staff via SNS.
Uses the status-createdAt GSI with a sort-key range instead of scanning the table."""
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
from boto3.dynamodb.conditions import Key

orders_table = boto3.resource("dynamodb").Table(os.environ["ORDERS_TABLE"])
sns = boto3.client("sns")
STATUS_INDEX = os.environ["STATUS_INDEX"]
TOPIC_ARN = os.environ["TOPIC_ARN"]

logger = logging.getLogger()
logger.setLevel(logging.INFO)

STATUSES = ["PLACED", "ACCEPTED", "PREPARING", "READY", "PICKED_UP", "REJECTED", "ABANDONED"]

try:
    from zoneinfo import ZoneInfo
    SHOP_TZ = ZoneInfo("America/Toronto")
except Exception:
    SHOP_TZ = timezone(timedelta(hours=-5))


def lambda_handler(event, context):
    # "Today" in the shop's time zone, converted to UTC (how createdAt is stored)
    start_local = datetime.now(SHOP_TZ).replace(hour=0, minute=0, second=0, microsecond=0)
    start_utc, end_utc = to_iso(start_local), to_iso(start_local + timedelta(days=1))

    counts, sales_cents, total = {}, 0, 0
    for status in STATUSES:
        orders = query_status(status, start_utc, end_utc)
        counts[status] = len(orders)
        total += len(orders)
        if status == "PICKED_UP":
            sales_cents = sum(int(o["totalCents"]) for o in orders)

    day = start_local.strftime("%A, %B %d")
    still_open = counts["PLACED"] + counts["ACCEPTED"] + counts["PREPARING"] + counts["READY"]
    message = "\n".join([
        f"Brewline daily summary - {day}", "",
        f"Total orders: {total}",
        f"Picked up: {counts['PICKED_UP']}  (${sales_cents / 100:.2f})",
        f"Rejected (out of stock): {counts['REJECTED']}",
        f"Abandoned (not picked up): {counts['ABANDONED']}",
        f"Still open: {still_open}",
    ])
    sns.publish(TopicArn=TOPIC_ARN, Subject=f"Brewline summary - {day}", Message=message)
    logger.info("Summary sent: %s", counts)
    return {"day": day, "counts": counts, "salesCents": sales_cents}


def query_status(status, start_utc, end_utc):
    items, args = [], {
        "IndexName": STATUS_INDEX,
        "KeyConditionExpression": Key("status").eq(status) & Key("createdAt").between(start_utc, end_utc),
    }
    while True:
        page = orders_table.query(**args)
        items.extend(page.get("Items", []))
        if "LastEvaluatedKey" not in page:
            return items
        args["ExclusiveStartKey"] = page["LastEvaluatedKey"]


def to_iso(dt):
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
