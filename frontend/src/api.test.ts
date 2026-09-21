import { afterEach, expect, it, vi } from "vitest";
import { loadDashboard } from "./api";

function metric(buildId: string) {
  return {
    state: "fresh",
    interval_start: "2026-09-20T11:00:00Z",
    interval_end: "2026-09-20T12:00:00Z",
    interval_semantics: "[start,end)",
    build: {
      build_id: buildId,
      dbt_invocation_id: "dbt-test",
      published_at: "2026-09-20T12:00:00Z",
      source_max_event_ts: null,
      source_max_ingested_at: null,
      source_event_count: 0,
      age_seconds: 0,
      state: "fresh",
    },
    metric_definition: "Measured definition",
    points: [],
  };
}

afterEach(() => vi.unstubAllGlobals());

it("retries once when endpoints straddle two analytics publications", async () => {
  const firstBuild = "00000000-0000-0000-0000-000000000001";
  const secondBuild = "00000000-0000-0000-0000-000000000002";
  let call = 0;
  const fetchMock = vi.fn(() => {
    const batch = Math.floor(call++ / 7);
    const position = (call - 1) % 7;
    const buildId = batch === 0 && position < 5 ? firstBuild : secondBuild;
    const payload =
      position < 5
        ? metric(buildId)
        : position === 5
          ? {
              state: "fresh",
              build: metric(secondBuild).build,
              running_build_started_at: null,
              latest_failed_build_at: null,
              latest_failure_reason: null,
              note: "Published",
            }
          : { items: [] };
    return Promise.resolve({ ok: true, json: () => Promise.resolve(payload) });
  });
  vi.stubGlobal("fetch", fetchMock);

  const result = await loadDashboard(24, "all");

  expect(fetchMock).toHaveBeenCalledTimes(14);
  expect(result.quality.build?.build_id).toBe(secondBuild);
  expect(result.payments.build.build_id).toBe(secondBuild);
});
