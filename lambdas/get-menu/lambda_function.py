"""GET /menu - returns active menu items (hidden items and stock counts are never exposed)."""
import json
import logging
import os

import boto3

TABLE_NAME = os.environ["MENU_TABLE"]
table = boto3.resource("dynamodb").Table(TABLE_NAME)  # created once, reused on warm starts

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    try:
        items, scan_args = [], {}
        while True:  # Scan returns max 1 MB per page
            page = table.scan(**scan_args)
            items.extend(page.get("Items", []))
            if "LastEvaluatedKey" not in page:
                break
            scan_args["ExclusiveStartKey"] = page["LastEvaluatedKey"]

        menu = []
        for item in items:
            if not item.get("active", False):
                continue  # hidden items never reach customers
            menu.append({
                "itemId": item["itemId"],
                "name": item["name"],
                "category": item["category"],
                "sizes": {size: int(price) for size, price in item["sizes"].items()},
                "soldOut": int(item.get("stock", 0)) <= 0,  # never expose exact stock
            })

        menu.sort(key=lambda m: (m["category"], m["name"]))  # scan order is random
        logger.info("get-menu returned %d items", len(menu))
        return respond(200, {"items": menu})

    except Exception:
        logger.exception("get-menu failed")
        return respond(500, {"error": {"code": "INTERNAL_ERROR",
                                       "message": "Could not load the menu"}})


def respond(status_code, body):
    return {"statusCode": status_code,
            "headers": {"Content-Type": "application/json"},
            "body": json.dumps(body)}
