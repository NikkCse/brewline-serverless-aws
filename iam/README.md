# IAM policies

Every Lambda has its **own execution role** = `AWSLambdaBasicExecutionRole` (logs) + one inline policy from this folder.
Each policy allows only the **actions** that function needs (e.g. `get-order` can only `GetItem`).

> **Note on `"Resource": "*"`:** in my sandbox account (an AWS Organizations member), resource-scoped ARNs were
> denied even when correct — I isolated it step by step (exact ARN → wildcards → `*`). As a documented exception,
> policies are scoped by **action** only. In a normal account each statement would list the exact table, index,
> topic, queue or bus ARN.
