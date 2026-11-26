"""Barista routes (JWT-protected at API Gateway):
  GET  /barista/orders?status=ACCEPTED|PREPARING|READY   -> queue, oldest first (GSI query)
  POST /barista/orders/{orderId}/status  {"action": ...}  -> state machine move (conditional update)
When an order becomes READY, a one-time EventBridge Scheduler timer is created to expire it."""
import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

orders_table = boto3.resource("dynamodb").Table(os.environ["ORDERS_TABLE"])
scheduler = boto3.client("scheduler")
STATUS_INDEX = os.environ["STATUS_INDEX"]
EXPIRE_FUNCTION_ARN = os.environ["EXPIRE_FUNCTION_ARN"]
SCHEDULER_ROLE_ARN = os.environ["SCHEDULER_ROLE_ARN"]
PICKUP_TIMEOUT_MINUTES = int(os.environ.get("PICKUP_TIMEOUT_MINUTES", "30"))

logger = logging.getLogger()
logger.setLevel(logging.INFO)

QUEUE_STATUSES = {"ACCEPTED", "PREPARING", "READY"}
TRANSITIONS = {  # action -> (required current status, new status)
    "START": ("ACCEPTED", "PREPARING"),
    "READY": ("PREPARING", "READY"),
    "PICKED_UP": ("READY", "PICKED_UP"),
}


def lambda_handler(event, context):
    route = event.get("routeKey", "")
    try:
        if route == "GET /barista/orders":
            return get_queue(event)
        if route == "POST /barista/orders/{orderId}/status":
            return change_status(event)
        return error(404, "NOT_FOUND", "Unknown route")
    except Exception:
        logger.exception("barista-api failed on %s", route)
        return error(500, "INTERNAL_ERROR", "Something went wrong")


def get_queue(event):
    status = ((event.get("queryStringParameters") or {}).get("status") or "ACCEPTED").upper()
    if status not in QUEUE_STATUSES:
        return error(400, "VALIDATION_ERROR", f"status must be one of {sorted(QUEUE_STATUSES)}")
    result = orders_table.query(IndexName=STATUS_INDEX,
                                KeyConditionExpression=Key("status").eq(status),
                                ScanIndexForward=True, Limit=50)  # oldest first
    orders = [{
        "orderId": o["orderId"], "status": o["status"], "pickupName": o["pickupName"],
        "items": [{"name": l["name"], "size": l["size"], "qty": int(l["qty"])} for l in o["items"]],
        "createdAt": o["createdAt"],
    } for o in result.get("Items", [])]
    logger.info("Queue %s: %d orders", status, len(orders))
    return respond(200, {"status": status, "orders": orders})


def change_status(event):
    order_id = (event.get("pathParameters") or {}).get("orderId", "")
    try:
        action = (json.loads(event.get("body") or "{}").get("action") or "").upper()
    except ValueError:
        return error(400, "VALIDATION_ERROR", "Body must be valid JSON")
    if action not in TRANSITIONS:
        return error(400, "VALIDATION_ERROR", f"action must be one of {list(TRANSITIONS)}")

    expected, new_status = TRANSITIONS[action]
    try:
        result = orders_table.update_item(
            Key={"orderId": order_id},
            UpdateExpression="SET #s = :new, updatedAt = :now",
            ConditionExpression="attribute_exists(orderId) AND #s = :expected",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":new": new_status, ":expected": expected, ":now": now_iso()},
            ReturnValues="ALL_NEW",
            ReturnValuesOnConditionCheckFailure="ALL_OLD",  # tells us WHY it failed
        )
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        current = e.response.get("Item")
        if not current:
            return error(404, "ORDER_NOT_FOUND", "Order not found")
        current_status = current["status"]["S"]
        return error(409, "INVALID_TRANSITION", f"Cannot {action} an order that is {current_status}")

    logger.info("Order %s: %s -> %s", order_id, expected, new_status)
    if new_status == "READY":
        start_pickup_timer(order_id)

    item = result["Attributes"]
    return respond(200, {"orderId": item["orderId"], "status": item["status"],
                         "updatedAt": item["updatedAt"]})


def start_pickup_timer(order_id):
    """One-time schedule that runs expire-order once, then deletes itself."""
    fire_at = datetime.now(timezone.utc) + timedelta(minutes=PICKUP_TIMEOUT_MINUTES)
    try:
        scheduler.create_schedule(
            Name=f"expire-{order_id}",
            ScheduleExpression=f"at({fire_at.strftime('%Y-%m-%dT%H:%M:%S')})",
            ScheduleExpressionTimezone="UTC",
            FlexibleTimeWindow={"Mode": "OFF"},
            Target={"Arn": EXPIRE_FUNCTION_ARN, "RoleArn": SCHEDULER_ROLE_ARN,
                    "Input": json.dumps({"orderId": order_id})},
            ActionAfterCompletion="DELETE",
        )
        logger.info("Pickup timer set for %s at %s UTC", order_id, fire_at.isoformat())
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConflictException":
            logger.info("Timer for %s already exists", order_id)
            return
        logger.exception("Could not set pickup timer for %s", order_id)  # don't fail the barista's click


def respond(code, body):
    return {"statusCode": code,
            "headers": {"Content-Type": "application/json", "Cache-Control": "no-store"},
            "body": json.dumps(body)}


def error(code, err_code, message):
    return respond(code, {"error": {"code": err_code, "message": message}})


def now_iso():
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"
