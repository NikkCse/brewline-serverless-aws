"""Triggered by EventBridge (OrderPlaced). Atomically reserves stock and ACCEPTS, or REJECTS when out of stock.
Idempotent consumer: duplicate deliveries are skipped because the order is no longer PLACED."""
import json
import logging
import os
from collections import defaultdict
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

MENU_TABLE = os.environ["MENU_TABLE"]
ORDERS_TABLE = os.environ["ORDERS_TABLE"]
EVENT_BUS = os.environ["EVENT_BUS_NAME"]

ddb = boto3.client("dynamodb")  # low-level client for transactions
dynamodb = boto3.resource("dynamodb")
menu_table = dynamodb.Table(MENU_TABLE)
orders_table = dynamodb.Table(ORDERS_TABLE)
events = boto3.client("events")

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    order_id = (event.get("detail") or {}).get("orderId")
    if not order_id:
        logger.warning("Event without orderId - ignoring (event id %s)", event.get("id"))
        return

    order = orders_table.get_item(Key={"orderId": order_id}, ConsistentRead=True).get("Item")
    if not order:
        logger.warning("Order %s not found - ignoring", order_id)
        return
    if order["status"] != "PLACED":  # EventBridge is at-least-once
        logger.info("Order %s is already %s - skipping duplicate event", order_id, order["status"])
        return

    needed = total_quantities(order["items"])
    stock_before = read_stock(needed)

    try:
        accept_order(order_id, needed)
    except ddb.exceptions.TransactionCanceledException as e:
        reasons = [r.get("Code") for r in e.response.get("CancellationReasons", [])]
        handle_cancelled(order, needed, reasons)
        return

    logger.info("Order %s ACCEPTED", order_id)
    publish("OrderAccepted", order_event(order, "ACCEPTED"))
    check_low_stock(needed, stock_before)


def total_quantities(lines):
    needed = defaultdict(int)
    for line in lines:
        needed[line["itemId"]] += int(line["qty"])
    return dict(needed)


def read_stock(needed):
    return {item_id: menu_table.get_item(Key={"itemId": item_id}).get("Item") for item_id in needed}


def accept_order(order_id, needed):
    """All-or-nothing: decrement every item's stock AND mark the order ACCEPTED."""
    actions = [{"Update": {
        "TableName": MENU_TABLE,
        "Key": {"itemId": {"S": item_id}},
        "UpdateExpression": "SET stock = stock - :q",
        "ConditionExpression": "stock >= :q",  # never go below zero
        "ExpressionAttributeValues": {":q": {"N": str(qty)}},
    }} for item_id, qty in needed.items()]
    actions.append({"Update": {
        "TableName": ORDERS_TABLE,
        "Key": {"orderId": {"S": order_id}},
        "UpdateExpression": "SET #s = :accepted, updatedAt = :now",
        "ConditionExpression": "#s = :placed",
        "ExpressionAttributeNames": {"#s": "status"},
        "ExpressionAttributeValues": {":accepted": {"S": "ACCEPTED"}, ":placed": {"S": "PLACED"},
                                      ":now": {"S": now_iso()}},
    }})
    ddb.transact_write_items(TransactItems=actions)


def handle_cancelled(order, needed, reasons):
    order_id = order["orderId"]
    item_ids = list(needed)
    if reasons and reasons[-1] == "ConditionalCheckFailed":
        logger.info("Order %s changed while processing - skipping", order_id)
        return
    short = [item_ids[i] for i, code in enumerate(reasons[:-1]) if code == "ConditionalCheckFailed"]
    if not short:
        raise RuntimeError(f"Transaction for {order_id} cancelled: {reasons}")  # let Lambda retry
    reject_order(order, short)


def reject_order(order, short_items):
    order_id = order["orderId"]
    try:
        orders_table.update_item(
            Key={"orderId": order_id},
            UpdateExpression="SET #s = :rejected, rejectReason = :r, unavailableItems = :u, updatedAt = :now",
            ConditionExpression="#s = :placed",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":rejected": "REJECTED", ":placed": "PLACED",
                                       ":r": "OUT_OF_STOCK", ":u": short_items, ":now": now_iso()})
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
            logger.info("Order %s changed while rejecting - skipping", order_id)
            return
        raise
    logger.info("Order %s REJECTED - out of stock: %s", order_id, ", ".join(short_items))
    publish("OrderRejected", order_event(order, "REJECTED", reason="OUT_OF_STOCK"))


def check_low_stock(needed, stock_before):
    for item_id, qty in needed.items():
        item = stock_before.get(item_id)
        if not item:
            continue
        before = int(item.get("stock", 0))
        after = before - qty
        limit = int(item.get("lowStockAt", 0))
        if after <= limit < before:  # alert only when crossing the line
            logger.info("LOW STOCK %s: %d left (alert at %d)", item_id, after, limit)
            publish("LowStock", {"schemaVersion": 1, "itemId": item_id, "stock": after,
                                 "lowStockAt": limit, "occurredAt": now_iso()})


def order_event(order, status, reason=None):
    detail = {"schemaVersion": 1, "orderId": order["orderId"], "status": status,
              "previousStatus": "PLACED", "itemCount": int(order["itemCount"]),
              "totalCents": int(order["totalCents"]), "occurredAt": now_iso()}
    if reason:
        detail["reason"] = reason
    return detail


def publish(detail_type, detail):
    try:
        result = events.put_events(Entries=[{"EventBusName": EVENT_BUS, "Source": "brewline.orders",
                                             "DetailType": detail_type, "Detail": json.dumps(detail)}])
        if result.get("FailedEntryCount", 0) > 0:
            logger.error("%s not published: %s", detail_type, result["Entries"][0].get("ErrorCode"))
        else:
            logger.info("Published %s", detail_type)
    except Exception:
        logger.exception("%s not published", detail_type)


def now_iso():
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"
