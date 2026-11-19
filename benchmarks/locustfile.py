import random
import time
import uuid
from locust import HttpUser, between, task


class TaskQueueUser(HttpUser):
    wait_time = between(0.05, 0.2)

    def on_start(self):
        # Rotate between valid API keys
        keys = ["bench-key-1", "dev-key-1", "test-key-1"]
        self.api_key = random.choice(keys)
        self.headers = {
            "Content-Type": "application/json",
            "X-API-Key": self.api_key,
        }
        self.submitted_jobs = []

    @task(5)
    def submit_cpu_hash_job(self):
        payload = {
            "task_type": "cpu_hash",
            "payload": {
                "iterations": random.choice([200, 500, 1000]),
                "input": f"locust-{uuid.uuid4().hex[:8]}",
            },
            "priority": random.randint(1, 10),
            "max_retries": 3,
        }
        with self.client.post(
            "/v1/jobs",
            json=payload,
            headers=self.headers,
            catch_response=True,
            name="POST /v1/jobs (cpu_hash)",
        ) as response:
            if response.status_code == 202:
                data = response.json()
                jid = data.get("job_id")
                if jid:
                    self.submitted_jobs.append(jid)
                response.success()
            elif response.status_code == 429:
                # Rate limited is expected under heavy concurrency
                response.success()
            else:
                response.failure(f"Unexpected status: {response.status_code} - {response.text}")

    @task(3)
    def submit_sleep_job(self):
        payload = {
            "task_type": "sleep",
            "payload": {"duration_sec": 0.05},
            "priority": random.randint(1, 5),
            "max_retries": 2,
        }
        with self.client.post(
            "/v1/jobs",
            json=payload,
            headers=self.headers,
            catch_response=True,
            name="POST /v1/jobs (sleep)",
        ) as response:
            if response.status_code in [202, 429]:
                response.success()
            else:
                response.failure(f"Unexpected status: {response.status_code}")

    @task(4)
    def poll_job_status(self):
        if not self.submitted_jobs:
            return
        # Pick one submitted job to inspect
        job_id = self.submitted_jobs.pop(0)
        with self.client.get(
            f"/v1/jobs/{job_id}",
            headers=self.headers,
            catch_response=True,
            name="GET /v1/jobs/{id}",
        ) as response:
            if response.status_code == 200:
                data = response.json()
                if data.get("status") not in ["COMPLETED", "DEAD_LETTERED"]:
                    # Still in progress; put back to re-poll later
                    if len(self.submitted_jobs) < 50:
                        self.submitted_jobs.append(job_id)
                response.success()
            elif response.status_code == 429:
                response.success()
            else:
                response.failure(f"Failed lookup: {response.status_code}")

    @task(1)
    def check_health_and_metrics(self):
        self.client.get("/health", name="GET /health")
        self.client.get("/metrics", name="GET /metrics")
        self.client.get("/v1/dlq", headers=self.headers, name="GET /v1/dlq")
