"""POST /orders - validates, prices server-side, enforces idempotency, saves PLACED, publishes OrderPlaced."""
import base64
import hashlib
import json
import logging
import os
import time
import uuid
from datetime import datetime, timezone

import boto3
from botocore.exceptions import ClientError

dynamodb = boto3.resource("dynamodb")
menu_table = dynamodb.Table(os.environ["MENU_TABLE"])
orders_table = dynamodb.Table(os.environ["ORDERS_TABLE"])
tickets_table = dynamodb.Table(os.environ["IDEMPOTENCY_TABLE"])
events = boto3.client("events")
EVENT_BUS = os.environ["EVENT_BUS_NAME"]

logger = logging.getLogger()
logger.setLevel(logging.INFO)

TICKET_TTL_SECONDS = 24 * 60 * 60  # idempotency tickets expire via DynamoDB TTL
MAX_LINES = 10
MAX_QTY = 10


class ApiError(Exception):
    """An expected problem reported to the client (bad input, conflict)."""
    def __init__(self, status, code, message):
        super().__init__(message)
        self.status, self.code, self.message = status, code, message


def lambda_handler(event, context):
    try:
        key = get_idempotency_key(event)
        pickup_name, lines = validate(parse_body(event))

        priced_lines, total = price_lines(lines)          # prices come from DynamoDB, never the browser
        request_hash = hash_request(pickup_name, lines)

        existing = claim_ticket(key, request_hash)         # idempotency check (conditional write)
        if existing is not None:
            return replay(key, existing, request_hash)

        logger.info("NEW key %s - creating order", key)
        try:
            order = save_order(pickup_name, priced_lines, total)
        except Exception:
            release_ticket(key)  # let the client retry with the same key
            raise

        publish_order_placed(order)

        body = {"orderId": order["orderId"], "status": order["status"],
                "itemCount": order["itemCount"], "totalCents": order["totalCents"],
                "createdAt": order["createdAt"]}
        complete_ticket(key, order["orderId"], body)
        logger.info("Order %s created for key %s", order["orderId"], key)
        return respond(201, body)

    except ApiError as e:
        logger.warning("Rejected: %s - %s", e.code, e.message)
        return respond(e.status, {"error": {"code": e.code, "message": e.message}})
    except Exception:
        logger.exception("create-order failed")
        return respond(500, {"error": {"code": "INTERNAL_ERROR",
                                       "message": "Could not place the order"}})


def get_idempotency_key(event):
    headers = {k.lower(): v for k, v in (event.get("headers") or {}).items()}
    key = (headers.get("idempotency-key") or "").strip()
    if not 8 <= len(key) <= 64:
        raise ApiError(400, "VALIDATION_ERROR", "Idempotency-Key header is required (8-64 characters)")
    return key


def parse_body(event):
    raw = event.get("body") or ""
    if event.get("isBase64Encoded"):
        raw = base64.b64decode(raw).decode("utf-8")
    try:
        data = json.loads(raw)
    except ValueError:
        raise ApiError(400, "VALIDATION_ERROR", "Body must be valid JSON")
    if not isinstance(data, dict):
        raise ApiError(400, "VALIDATION_ERROR", "Body must be a JSON object")
    return data


def validate(data):
    name = data.get("pickupName")
    if not isinstance(name, str) or not 1 <= len(name.strip()) <= 30:
        raise ApiError(400, "VALIDATION_ERROR", "pickupName must be 1-30 characters")
    items = data.get("items")
    if not isinstance(items, list) or not 1 <= len(items) <= MAX_LINES:
        raise ApiError(400, "VALIDATION_ERROR", f"items must be a list of 1-{MAX_LINES}")

    lines = []
    for line in items:
        if not isinstance(line, dict):
            raise ApiError(400, "VALIDATION_ERROR", "Each item must be an object")
        item_id, size, qty = line.get("itemId"), line.get("size"), line.get("qty")
        if not isinstance(item_id, str) or not isinstance(size, str):
            raise ApiError(400, "VALIDATION_ERROR", "Each item needs itemId and size")
        if isinstance(qty, bool) or not isinstance(qty, int) or not 1 <= qty <= MAX_QTY:
            raise ApiError(400, "VALIDATION_ERROR", f"qty must be 1-{MAX_QTY}")
        lines.append({"itemId": item_id, "size": size.upper(), "qty": qty})
    return name.strip(), lines


def price_lines(lines):
    cache, priced, total = {}, [], 0
    for line in lines:
        item_id = line["itemId"]
        if item_id not in cache:
            cache[item_id] = menu_table.get_item(Key={"itemId": item_id}).get("Item")
        item = cache[item_id]
        if not item or not item.get("active", False):
            raise ApiError(400, "UNKNOWN_ITEM", f"Unknown item: {item_id}")
        if line["size"] not in item["sizes"]:
            raise ApiError(400, "VALIDATION_ERROR", f"{item_id} has no size {line['size']}")
        unit = int(item["sizes"][line["size"]])
        line_total = unit * line["qty"]
        total += line_total
        priced.append({**line, "name": item["name"], "unitCents": unit, "lineCents": line_total})
    return priced, total


def hash_request(pickup_name, lines):
    text = json.dumps({"pickupName": pickup_name, "items": lines}, sort_keys=True)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def claim_ticket(key, request_hash):
    """Returns None if the ticket is new, or the existing ticket if seen before."""
    try:
        tickets_table.put_item(
            Item={"idempotencyKey": key, "requestHash": request_hash, "status": "IN_PROGRESS",
                  "expiresAt": int(time.time()) + TICKET_TTL_SECONDS},
            # check-and-save in ONE atomic step, so two simultaneous taps can't both win
            ConditionExpression="attribute_not_exists(idempotencyKey) OR #s = :failed",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":failed": "FAILED"},
        )
        return None
    except ClientError as e:
        if e.response["Error"]["Code"] != "ConditionalCheckFailedException":
            raise
        return tickets_table.get_item(Key={"idempotencyKey": key}, ConsistentRead=True).get("Item", {})


def replay(key, ticket, request_hash):
    if ticket.get("requestHash") != request_hash:
        raise ApiError(409, "IDEMPOTENCY_CONFLICT",
                       "This Idempotency-Key was already used for a different order")
    if ticket.get("status") != "COMPLETED":
        raise ApiError(409, "IDEMPOTENCY_CONFLICT", "This order is still being processed, retry shortly")
    logger.info("DUPLICATE key %s - returning existing order %s", key, ticket["orderId"])
    return respond(201, json.loads(ticket["response"]), replay=True)


def save_order(pickup_name, priced_lines, total):
    now = datetime.now(timezone.utc)
    order = {
        "orderId": "ord_" + uuid.uuid4().hex[:12],
        "status": "PLACED",
        "pickupName": pickup_name,
        "items": priced_lines,
        "itemCount": sum(l["qty"] for l in priced_lines),
        "totalCents": total,
        "createdAt": now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z",
        "eventPublished": False,  # flipped to True once OrderPlaced is on the bus (sweeper uses this)
    }
    orders_table.put_item(Item=order, ConditionExpression="attribute_not_exists(orderId)")
    return order


def publish_order_placed(order):
    """Announce the order on the bus. Never fails the customer's request."""
    detail = {"schemaVersion": 1, "orderId": order["orderId"], "status": order["status"],
              "previousStatus": None, "itemCount": order["itemCount"],
              "totalCents": order["totalCents"], "occurredAt": order["createdAt"]}
    try:
        result = events.put_events(Entries=[{
            "EventBusName": EVENT_BUS, "Source": "brewline.orders",
            "DetailType": "OrderPlaced", "Detail": json.dumps(detail)}])
        if result.get("FailedEntryCount", 0) > 0:  # PutEvents can fail without raising
            logger.error("OrderPlaced not published for %s: %s",
                         order["orderId"], result["Entries"][0].get("ErrorCode"))
            return
        orders_table.update_item(Key={"orderId": order["orderId"]},
                                 UpdateExpression="SET eventPublished = :t",
                                 ExpressionAttributeValues={":t": True})
        logger.info("Published OrderPlaced for %s", order["orderId"])
    except Exception:
        logger.exception("OrderPlaced not published for %s", order["orderId"])


def complete_ticket(key, order_id, body):
    tickets_table.update_item(
        Key={"idempotencyKey": key},
        UpdateExpression="SET #s = :done, orderId = :o, #r = :r",
        ExpressionAttributeNames={"#s": "status", "#r": "response"},
        ExpressionAttributeValues={":done": "COMPLETED", ":o": order_id, ":r": json.dumps(body)})


def release_ticket(key):
    try:
        tickets_table.update_item(
            Key={"idempotencyKey": key}, UpdateExpression="SET #s = :failed",
            ExpressionAttributeNames={"#s": "status"},
            ExpressionAttributeValues={":failed": "FAILED"})
    except Exception:
        logger.exception("Could not release ticket %s", key)


def respond(status_code, body, replay=False):
    headers = {"Content-Type": "application/json"}
    if replay:
        headers["Idempotent-Replay"] = "true"
    return {"statusCode": status_code, "headers": headers, "body": json.dumps(body)}
