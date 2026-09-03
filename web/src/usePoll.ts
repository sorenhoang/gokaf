import { useEffect, useRef, useState } from "react";

// usePoll re-runs fn every intervalMs and exposes the latest value, the last
// error, and a manual refresh(). No state library — just useState + setInterval.
export function usePoll<T>(fn: () => Promise<T>, intervalMs = 1000) {
  const [data, setData] = useState<T | null>(null);
  const [error, setError] = useState<string | null>(null);
  const fnRef = useRef(fn);
  fnRef.current = fn;
  const [tick, setTick] = useState(0);

  useEffect(() => {
    let alive = true;
    const run = async () => {
      try {
        const next = await fnRef.current();
        if (alive) {
          setData(next);
          setError(null);
        }
      } catch (e) {
        if (alive) setError(e instanceof Error ? e.message : String(e));
      }
    };
    run();
    const id = setInterval(run, intervalMs);
    return () => {
      alive = false;
      clearInterval(id);
    };
  }, [intervalMs, tick]);

  return { data, error, refresh: () => setTick((t) => t + 1) };
}
