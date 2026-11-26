"""GET /orders/{orderId} - returns the public view of one order (used for status polling)."""
import json
import logging
import os
import re

import boto3

orders_table = boto3.resource("dynamodb").Table(os.environ["ORDERS_TABLE"])
logger = logging.getLogger()
logger.setLevel(logging.INFO)

ORDER_ID_PATTERN = re.compile(r"^ord_[0-9a-f]{12}$")


def lambda_handler(event, context):
    order_id = (event.get("pathParameters") or {}).get("orderId", "")
    if not ORDER_ID_PATTERN.match(order_id):  # validate before touching the database
        return respond(400, {"error": {"code": "VALIDATION_ERROR", "message": "Invalid order ID"}})
    try:
        order = orders_table.get_item(Key={"orderId": order_id}).get("Item")
    except Exception:
        logger.exception("get-order failed for %s", order_id)
        return respond(500, {"error": {"code": "INTERNAL_ERROR", "message": "Could not load the order"}})

    if not order:
        return respond(404, {"error": {"code": "ORDER_NOT_FOUND", "message": "Order not found"}})

    logger.info("Order %s is %s", order_id, order["status"])
    return respond(200, to_public(order))


def to_public(order):
    """Only the fields the status page needs (internal flags stay hidden)."""
    body = {
        "orderId": order["orderId"],
        "status": order["status"],
        "pickupName": order["pickupName"],
        "items": [{"name": l["name"], "size": l["size"], "qty": int(l["qty"]),
                   "lineCents": int(l["lineCents"])} for l in order["items"]],
        "itemCount": int(order["itemCount"]),
        "totalCents": int(order["totalCents"]),
        "createdAt": order["createdAt"],
        "updatedAt": order.get("updatedAt", order["createdAt"]),
    }
    if order.get("rejectReason"):
        body["rejectReason"] = order["rejectReason"]
        body["unavailableItems"] = list(order.get("unavailableItems", []))
    return body


def respond(status_code, body):
    return {"statusCode": status_code,
            "headers": {"Content-Type": "application/json", "Cache-Control": "no-store"},
            "body": json.dumps(body)}
