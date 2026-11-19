"""Live container failover benchmark."""
import asyncio
import json
import subprocess
import time
import httpx
import redis.asyncio as aioredis


async def run_failover_benchmark():
    gateway_url = "http://127.0.0.1:8000"
    headers = {
        "Content-Type": "application/json",
        "X-API-Key": "dev-key-1",
    }
    redis = aioredis.from_url("redis://127.0.0.1:6379/0", decode_responses=True)

    print("--- [Failover Benchmark] Starting ---")

    # Step 1: Submit long-running sleep job
    sleep_duration = 8.0
    payload = {
        "task_type": "sleep",
        "payload": {"duration_sec": sleep_duration},
        "priority": 10,
        "max_retries": 3,
    }

    async with httpx.AsyncClient(timeout=10.0) as client:
        resp = await client.post(f"{gateway_url}/v1/jobs", json=payload, headers=headers)
        assert resp.status_code == 202, f"Failed to submit: {resp.text}"
        job_id = resp.json()["job_id"]
        print(f"Submitted job {job_id} with duration={sleep_duration}s")

        # Step 2: Poll until job is RUNNING and assigned to a worker
        assigned_worker = None
        for _ in range(50):
            await asyncio.sleep(0.2)
            res = await client.get(f"{gateway_url}/v1/jobs/{job_id}", headers=headers)
            if res.status_code == 200:
                data = res.json()
                if data["status"] == "RUNNING" and data["assigned_worker"]:
                    assigned_worker = data["assigned_worker"]
                    break

        assert assigned_worker, "Job did not enter RUNNING state in time"
        print(f"Job is RUNNING on initial worker: {assigned_worker}")

        # Step 3: Find container ID for assigned worker from Redis
        active_workers = await redis.hgetall("workers:active")
        worker_addr = active_workers.get(assigned_worker)
        assert worker_addr, f"Worker {assigned_worker} not found in workers:active"
        container_id = worker_addr.split(":")[0]
        print(f"Worker {assigned_worker} corresponds to container ID {container_id}")

        # Step 4: Kill the worker container
        print(f"Executing: docker kill {container_id}")
        t_kill = time.time()
        kill_proc = subprocess.run(
            ["wsl", "-d", "Ubuntu", "-e", "docker", "kill", container_id],
            capture_output=True,
            text=True,
        )
        assert kill_proc.returncode == 0, f"docker kill failed: {kill_proc.stderr}"
        print(f"Successfully killed worker container {container_id} at {t_kill:.3f}")

        # Step 5: Poll until job reaches COMPLETED state on surviving worker
        surviving_worker = None
        final_status = None
        t_completed = None

        for _ in range(120):
            await asyncio.sleep(0.5)
            res = await client.get(f"{gateway_url}/v1/jobs/{job_id}", headers=headers)
            if res.status_code == 200:
                data = res.json()
                status = data["status"]
                worker = data["assigned_worker"]
                if status == "COMPLETED":
                    final_status = status
                    surviving_worker = worker
                    t_completed = time.time()
                    break

        await redis.aclose()

        assert final_status == "COMPLETED", f"Job failed to complete. Final status: {final_status}"
        recovery_time = t_completed - t_kill

        print("\n=== Failover Benchmark Results ===")
        print(f"Initial Worker:      {assigned_worker} (Container {container_id})")
        print(f"Killed at:           {t_kill:.3f}")
        print(f"Completed at:        {t_completed:.3f}")
        print(f"Total Recovery Time: {recovery_time:.2f}s")
        print(f"Surviving Worker:    {surviving_worker}")
        print(f"Final Status:        {final_status}")
        print(f"Pass Criterion met:  {recovery_time <= 15.0} (<= 15s)")

        return {
            "job_id": job_id,
            "initial_worker": assigned_worker,
            "container_id": container_id,
            "surviving_worker": surviving_worker,
            "recovery_time_sec": round(recovery_time, 2),
            "pass_criterion_met": recovery_time <= 15.0,
        }


if __name__ == "__main__":
    asyncio.run(run_failover_benchmark())
