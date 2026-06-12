# Async Job Queue API Usage

## Full Async Pattern

The system provides a complete async workflow: **enqueue → poll → retrieve results**.

### 1. Enqueue a Job

```bash
curl -X POST http://localhost:8000/score \
  -H "Content-Type: application/json" \
  -d '{"text": "sample data"}'
```

Response:
```json
{
  "job_id": "7bb53fa9-5374-4fab-9969-62a070067a3f",
  "queued": true
}
```

### 2. Poll Job Status

Use the `job_id` to check status:

```bash
curl http://localhost:8000/job/7bb53fa9-5374-4fab-9969-62a070067a3f
```

#### Status Responses

**Pending** (job waiting in queue):
```json
{
  "job_id": "7bb53fa9-5374-4fab-9969-62a070067a3f",
  "status": "pending",
  "result": null,
  "error": null,
  "retries": 0
}
```

**Completed** (job processed successfully):
```json
{
  "job_id": "7bb53fa9-5374-4fab-9969-62a070067a3f",
  "status": "completed",
  "result": {"score": 0.42},
  "error": null,
  "retries": 0
}
```

**Retrying** (job failed but scheduled to retry):
```json
{
  "job_id": "7bb53fa9-5374-4fab-9969-62a070067a3f",
  "status": "retrying",
  "result": null,
  "error": "connection timeout",
  "retries": 2
}
```

**Failed** (job exhausted retries, in dead-letter queue):
```json
{
  "job_id": "7bb53fa9-5374-4fab-9969-62a070067a3f",
  "status": "failed",
  "result": null,
  "error": "max retries exceeded",
  "retries": 3
}
```

**Unknown** (job not found):
```
404 Not Found
{"detail": "Job <job_id> not found"}
```

## Polling Strategy

For client-side polling, recommended approach:

```python
import time

# Enqueue
response = client.post('/score', json=payload)
job_id = response.json()['job_id']

# Poll with exponential backoff
retry_count = 0
max_retries = 30
wait_time = 0.5

while retry_count < max_retries:
    response = client.get(f'/job/{job_id}')
    
    if response.status_code == 404:
        print("Job not found")
        break
    
    status = response.json()
    
    if status['status'] == 'completed':
        print(f"Result: {status['result']}")
        break
    elif status['status'] == 'failed':
        print(f"Failed: {status['error']}")
        break
    
    time.sleep(wait_time)
    wait_time = min(wait_time * 1.5, 5)  # exponential backoff, max 5s
    retry_count += 1
```

## Result Persistence

- **In-memory**: Results stored in Redis hash `job:results` (fast, volatile)
- **Durable**: Results persisted to PostgreSQL `job_results` table (survives restarts)
- **Query DB directly**: 
  ```sql
  SELECT id, payload, created_at FROM job_results 
  WHERE id = '7bb53fa9-5374-4fab-9969-62a070067a3f';
  ```

## Observability

Monitor queue depth:
```bash
curl http://localhost:8000/  # (add health/metrics endpoint for queue stats)
```

Check PostgreSQL results:
```bash
psql "postgresql://postgres:devin@localhost/async-workflow" -c \
  "SELECT id, payload, created_at FROM job_results ORDER BY created_at DESC LIMIT 10;"
```

Check Redis in-memory results:
```bash
redis-cli HGETALL "job:results"
redis-cli LLEN "queue:jobs"
redis-cli ZCARD "queue:jobs:retry"
redis-cli LLEN "queue:jobs:dlq"
```
