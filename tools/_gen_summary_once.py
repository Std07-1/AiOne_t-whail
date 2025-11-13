from pathlib import Path

from tools import unified_runner as ur


def main():
    out_dir = Path("reports/run_8h")
    cfg = ur.RunnerConfig(mode="postprocess", out_dir=out_dir)
    cfg.forward_profiles = ["strong", "soft", "explain", "hint", "quality"]
    orch = ur.RunnerOrchestrator(cfg)
    p = orch.generate_summary()
    # Copy to canonical summary.md
    (out_dir / "summary.md").write_text(
        p.read_text(encoding="utf-8", errors="ignore"), encoding="utf-8"
    )
    print("Generated summary at", p)
    print("First 40 lines:")
    print("\n".join(p.read_text(encoding="utf-8", errors="ignore").splitlines()[:40]))


if __name__ == "__main__":
    main()
