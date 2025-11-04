import asyncio
import sys


async def run_for(duration_sec: int, scheduler_enabled: bool) -> None:
    import config.config as cfg
    from app.main import run_pipeline

    # toggle feature flag before pipeline starts
    try:
        cfg.STAGE2_SCHEDULER_ENABLED = bool(scheduler_enabled)
    except Exception:
        pass

    print(
        f"\n=== RUN start: {duration_sec}s, STAGE2_SCHEDULER_ENABLED={scheduler_enabled} ===",
        flush=True,
    )

    # Run pipeline as a task we can cancel
    task = asyncio.create_task(run_pipeline())
    try:
        await asyncio.wait_for(task, timeout=duration_sec)
    except TimeoutError:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        print(f"=== RUN finished by timeout ({duration_sec}s) ===", flush=True)
    except Exception as e:
        print(f"=== RUN aborted by error: {e} ===", flush=True)
        # Make sure to cancel on unexpected errors too
        task.cancel()
        try:
            await task
        except Exception:
            pass
        raise


async def main() -> None:
    # First run: 30 seconds with scheduler disabled
    await run_for(30, scheduler_enabled=False)
    # Small pause between runs
    await asyncio.sleep(2)
    # Second run: 30 seconds with scheduler enabled
    await run_for(30, scheduler_enabled=True)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        sys.exit(130)
