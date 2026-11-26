"""Runs every 5 minutes. Finds PLACED orders whose OrderPlaced event was never published
(eventPublished = false) and re-publishes it. Safe to re-run: process-order is an idempotent consumer.
Note: Scan is fine for a demo-sized table; in production use a sparse GSI on unpublished orders."""
import json
import logging
import os
from datetime import datetime, timedelta, timezone

import boto3
from boto3.dynamodb.conditions import Attr

orders_table = boto3.resource("dynamodb").Table(os.environ["ORDERS_TABLE"])
events = boto3.client("events")
EVENT_BUS = os.environ["EVENT_BUS_NAME"]

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    # Skip brand-new orders: create-order may still be publishing them
    cutoff = (datetime.now(timezone.utc) - timedelta(minutes=2)).strftime("%Y-%m-%dT%H:%M:%S")

    stuck, args = [], {"FilterExpression": Attr("eventPublished").eq(False)
                                           & Attr("status").eq("PLACED")
                                           & Attr("createdAt").lt(cutoff)}
    while True:
        page = orders_table.scan(**args)
        stuck.extend(page.get("Items", []))
        if "LastEvaluatedKey" not in page:
            break
        args["ExclusiveStartKey"] = page["LastEvaluatedKey"]

    sent = 0
    for order in stuck:
        detail = {"schemaVersion": 1, "orderId": order["orderId"], "status": "PLACED",
                  "previousStatus": None, "itemCount": int(order["itemCount"]),
                  "totalCents": int(order["totalCents"]), "occurredAt": order["createdAt"]}
        result = events.put_events(Entries=[{"EventBusName": EVENT_BUS, "Source": "brewline.orders",
                                             "DetailType": "OrderPlaced", "Detail": json.dumps(detail)}])
        if result.get("FailedEntryCount", 0) > 0:
            logger.error("Still could not publish %s - will retry next run", order["orderId"])
            continue
        orders_table.update_item(Key={"orderId": order["orderId"]},
                                 UpdateExpression="SET eventPublished = :t",
                                 ExpressionAttributeValues={":t": True})
        sent += 1
        logger.info("Re-published OrderPlaced for %s", order["orderId"])

    logger.info("Sweeper done: %d found, %d re-published", len(stuck), sent)
    return {"found": len(stuck), "republished": sent}
