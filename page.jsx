"use client";

/**
 * src/app/watch/[itemId]/page.jsx
 *
 * Plyr + HLS.js player with automatic multi-origin failover.
 *
 * How failover works (no page reload, same <video> element, same Plyr UI):
 *   1. GET /api/assets/:id/manifest -> origin nodes in priority order with signed URLs.
 *   2. Play origin[0]. A watchdog arms at start-up; it is disarmed once the FIRST SEGMENT is
 *      buffered (not just the manifest), so an origin that serves playlists but hangs on
 *      segments still fails over.
 *   3. Triggers: fatal hls.js error, start-up timeout, or a mid-playback stall that lasts
 *      longer than TUNING.stallTimeoutMs.
 *   4. On failover we remember currentTime + play/pause, tear down only the hls.js engine,
 *      and start the next untried origin at that position.
 *   5. Signed URLs are refreshed in the background before they expire, so long lectures
 *      keep working. If every origin fails you get a "Try again" button.
 *
 * Requires: npm i plyr hls.js   (hls.js ^1.5 for the *LoadPolicy config)
 * Env:      NEXT_PUBLIC_API_BASE=https://api.example.com
 * Note:     origins must send CORS headers (Access-Control-Allow-Origin) for hls.js XHR.
 */

import { useEffect, useRef, useState } from "react";
import { useParams } from "next/navigation";
import "plyr/dist/plyr.css";

const API_BASE = process.env.NEXT_PUBLIC_API_BASE || "";

const TUNING = {
  startupTimeoutMs: 10_000, // no first segment by then -> next origin
  stallTimeoutMs: 12_000, // continuous buffering this long mid-playback -> next origin
  refreshLeadMs: 120_000, // refresh signed URLs this long before they expire
  noticeMs: 6_000, // how long the "switched origin" notice stays visible
};

const INITIAL_STATE = {
  phase: "loading", // loading | ready | error
  title: "",
  origin: "",
  levels: [],
  level: -1, // -1 = automatic adaptive bitrate
  notice: "",
  message: "",
};

/** Replace with however your app stores the access token (NextAuth, cookie, etc.). */
function getAccessToken() {
  try {
    return window.localStorage.getItem("access_token");
  } catch {
    return null;
  }
}

async function fetchManifest(itemId, signal) {
  const token = getAccessToken();
  const res = await fetch(`${API_BASE}/api/assets/${encodeURIComponent(itemId)}/manifest`, {
    headers: token ? { Authorization: `Bearer ${token}` } : {},
    cache: "no-store",
    signal,
  });
  const fatal = (message) => Object.assign(new Error(message), { fatal: true });
  if (res.status === 401) throw fatal("Please sign in to watch this lesson.");
  if (res.status === 403) throw fatal("You're not enrolled in this course.");
  if (res.status === 404) throw fatal("This lesson could not be found.");
  if (!res.ok) throw new Error(`Manifest request failed (${res.status})`);
  return res.json();
}

/** hls.js load policy: fail fast so a dead origin is abandoned in seconds, not minutes. */
const policy = (ttfbMs, totalMs, timeoutRetries, errorRetries) => ({
  default: {
    maxTimeToFirstByteMs: ttfbMs,
    maxLoadTimeMs: totalMs,
    timeoutRetry: { maxNumRetry: timeoutRetries, retryDelayMs: 0, maxRetryDelayMs: 0 },
    errorRetry: { maxNumRetry: errorRetries, retryDelayMs: 500, maxRetryDelayMs: 4000 },
  },
});

export default function WatchPage() {
  const { itemId } = useParams();
  const holderRef = useRef(null); // React never manages children here; we mount <video> by hand
  const ctrl = useRef(null); // exposes the live engine to event handlers (quality select)
  const [state, setState] = useState(INITIAL_STATE);
  const [attempt, setAttempt] = useState(0); // bump to restart from scratch

  useEffect(() => {
    const holder = holderRef.current;
    if (!holder || !itemId) return undefined;

    let disposed = false;
    const abort = new AbortController();
    const R = {
      Hls: null,
      hls: null,
      player: null,
      video: null,
      origins: [],
      idx: 0,
      failed: new Set(), // origins that failed since the last healthy moment
      healthy: false, // true once the current origin delivered a segment
      mediaRecovered: false,
      resume: { time: 0, playing: false },
      expiresAt: 0,
      timers: {},
    };
    ctrl.current = R;
    setState(INITIAL_STATE);

    const patch = (p) => {
      if (!disposed) setState((s) => ({ ...s, ...p }));
    };
    const setTimer = (name, fn, ms) => {
      clearTimeout(R.timers[name]);
      R.timers[name] = setTimeout(fn, ms);
    };
    const clearTimer = (name) => {
      clearTimeout(R.timers[name]);
      R.timers[name] = null;
    };
    const notice = (text) => {
      patch({ notice: text });
      setTimer("notice", () => patch({ notice: "" }), TUNING.noticeMs);
    };

    /** Stop the current engine only; the <video> element and Plyr UI stay alive. */
    const teardownEngine = () => {
      clearTimer("startup");
      clearTimer("stall");
      if (R.hls) {
        try {
          R.hls.destroy();
        } catch {
          /* already destroyed */
        }
        R.hls = null;
      }
      if (R.video) {
        R.video.removeAttribute("src");
        R.video.load();
      }
    };

    const giveUp = (message) => {
      teardownEngine();
      patch({ phase: "error", message });
    };

    const markHealthy = () => {
      if (R.healthy) return;
      R.healthy = true;
      R.failed.clear(); // a working origin resets the blacklist so we can cycle later
      clearTimer("startup");
      patch({ phase: "ready" });
    };

    /** Abandon the current origin and continue from the same position on the next one. */
    const failover = (reason) => {
      if (disposed) return;
      const video = R.video;
      if (video) {
        R.resume = {
          time: video.currentTime || R.resume.time || 0,
          // If we never became healthy, keep the user's earlier intent to play.
          playing: !video.paused || (!R.healthy && R.resume.playing),
        };
      }
      const failedName = R.origins[R.idx]?.name;
      R.failed.add(R.idx);
      console.warn(`[player] ${failedName} failed: ${reason}`);

      const next = R.origins.findIndex((_, i) => !R.failed.has(i));
      if (next === -1) {
        giveUp("All origin nodes are unreachable right now. Please try again in a moment.");
        return;
      }
      notice(`${failedName} didn't respond - switched to ${R.origins[next].name}.`);
      startOrigin(next);
    };

    /** hls.js loader that appends the current signed query to same-origin requests. */
    const makeLoader = (origin) => {
      const host = new URL(origin.url).host;
      const Base = R.Hls.DefaultConfig.loader;
      const currentQuery = () => (R.origins.find((o) => o.name === origin.name) || origin).query;
      return class SignedLoader extends Base {
        load(context, config, callbacks) {
          try {
            const u = new URL(context.url);
            if (u.host === host && !u.searchParams.has("sig")) {
              const q = currentQuery();
              u.search = u.search ? `${u.search.slice(1)}&${q}` : q;
              context.url = u.toString();
            }
          } catch {
            /* leave URL untouched */
          }
          super.load(context, config, callbacks);
        }
      };
    };

    const startOrigin = (i) => {
      const { Hls } = R;
      const video = R.video;
      const origin = R.origins[i];
      R.idx = i;
      R.healthy = false;
      R.mediaRecovered = false;
      teardownEngine();
      patch({ phase: "loading", origin: origin.name, levels: [], level: -1 });
      setTimer("startup", () => failover("start-up timeout"), TUNING.startupTimeoutMs);

      if (Hls.isSupported()) {
        const hls = new Hls({
          loader: makeLoader(origin),
          startPosition: R.resume.time > 1 ? R.resume.time : -1,
          capLevelToPlayerSize: true,
          maxBufferLength: 30,
          manifestLoadPolicy: policy(8000, 10000, 1, 1),
          playlistLoadPolicy: policy(8000, 10000, 1, 1),
          fragLoadPolicy: policy(10000, 30000, 2, 3),
        });
        R.hls = hls;

        hls.on(Hls.Events.MANIFEST_PARSED, (_e, data) => {
          patch({
            levels: data.levels.map((l, index) => ({
              index,
              label: l.height ? `${l.height}p` : `${Math.round(l.bitrate / 1000)} kbps`,
            })),
          });
          if (R.resume.playing) video.play().catch(() => {});
        });
        hls.on(Hls.Events.FRAG_BUFFERED, markHealthy);
        hls.on(Hls.Events.ERROR, (_e, data) => {
          if (!data.fatal) return; // hls.js already retries non-fatal errors itself
          if (data.type === Hls.ErrorTypes.MEDIA_ERROR && !R.mediaRecovered) {
            R.mediaRecovered = true; // one in-place recovery attempt before switching origin
            hls.recoverMediaError();
            return;
          }
          failover(`${data.type}/${data.details}`);
        });

        hls.loadSource(origin.url);
        hls.attachMedia(video);
      } else if (video.canPlayType("application/vnd.apple.mpegurl")) {
        // Native HLS (older Safari). Only the manifest URL carries the signature here, so
        // your edge must also accept the signature for segments (cookie or path token).
        video.addEventListener(
          "loadedmetadata",
          () => {
            if (R.resume.time > 1) video.currentTime = R.resume.time;
            if (R.resume.playing) video.play().catch(() => {});
          },
          { once: true }
        );
        video.addEventListener("canplay", markHealthy, { once: true });
        video.src = origin.url;
      } else {
        giveUp("Your browser can't play this stream.");
      }
    };

    /** Re-fetch the manifest before signatures expire; new queries are picked up per request. */
    const scheduleRefresh = () => {
      const wait = Math.max(5_000, R.expiresAt - Date.now() - TUNING.refreshLeadMs);
      setTimer("refresh", async () => {
        try {
          const fresh = await fetchManifest(itemId, abort.signal);
          if (disposed) return;
          for (const o of fresh.origins) {
            const mine = R.origins.find((x) => x.name === o.name);
            if (mine) {
              mine.url = o.url;
              mine.query = o.query;
            }
          }
          R.expiresAt = Date.parse(fresh.expires_at);
        } catch {
          if (disposed) return;
          R.expiresAt = Date.now() + TUNING.refreshLeadMs + 30_000; // retry in ~30s
        }
        scheduleRefresh();
      }, wait);
    };

    const attachVideoListeners = (video) => {
      // Mid-playback stall watchdog (start-up is covered by the start-up timer).
      video.addEventListener("waiting", () => {
        if (R.healthy) setTimer("stall", () => failover("stalled"), TUNING.stallTimeoutMs);
      });
      ["playing", "canplay", "seeked", "pause", "emptied"].forEach((ev) =>
        video.addEventListener(ev, () => clearTimer("stall"))
      );
      video.addEventListener("error", () => {
        if (!R.hls && video.getAttribute("src")) failover("native playback error");
      });
    };

    (async () => {
      try {
        const [manifest, plyrMod, hlsMod] = await Promise.all([
          fetchManifest(itemId, abort.signal),
          import("plyr"), // dynamic: both libraries touch `window`, so never load them during SSR
          import("hls.js"),
        ]);
        if (disposed) return;
        if (!manifest.origins?.length) {
          throw Object.assign(new Error("No playable source is available for this lesson."), { fatal: true });
        }

        R.Hls = hlsMod.default;
        R.origins = manifest.origins;
        R.expiresAt = Date.parse(manifest.expires_at);

        const video = document.createElement("video");
        video.setAttribute("playsinline", "");
        video.preload = "metadata";
        holder.appendChild(video);
        R.video = video;

        const Plyr = plyrMod.default;
        R.player = new Plyr(video, {
          controls: ["play-large", "play", "progress", "current-time", "mute", "volume", "settings", "pip", "fullscreen"],
          settings: ["speed"],
          ratio: "16:9",
          keyboard: { focused: true, global: false },
        });

        attachVideoListeners(video);
        patch({ title: manifest.title || "" });
        scheduleRefresh();
        startOrigin(0);
      } catch (err) {
        if (disposed || err.name === "AbortError") return;
        patch({
          phase: "error",
          message: err.fatal ? err.message : "Couldn't load this lesson. Check your connection and try again.",
        });
      }
    })();

    return () => {
      disposed = true;
      abort.abort();
      Object.values(R.timers).forEach((t) => clearTimeout(t));
      teardownEngine();
      try {
        R.player?.destroy();
      } catch {
        /* ignore */
      }
      holder.replaceChildren();
      ctrl.current = null;
    };
  }, [itemId, attempt]);

  const onQualityChange = (e) => {
    const level = Number(e.target.value);
    if (ctrl.current?.hls) ctrl.current.hls.currentLevel = level; // -1 re-enables auto ABR
    setState((s) => ({ ...s, level }));
  };

  return (
    <main style={styles.page}>
      <h1 style={styles.title}>{state.title || "Lesson"}</h1>

      <div style={styles.stage}>
        <div ref={holderRef} style={{ width: "100%" }} />
        {state.phase === "loading" && (
          <div style={{ ...styles.overlay, pointerEvents: "none" }} role="status">
            Connecting{state.origin ? ` to ${state.origin}` : ""}...
          </div>
        )}
        {state.phase === "error" && (
          <div style={styles.overlay} role="alert">
            <p style={{ margin: "0 0 12px" }}>{state.message}</p>
            <button style={styles.button} onClick={() => setAttempt((a) => a + 1)}>
              Try again
            </button>
          </div>
        )}
      </div>

      <div style={styles.bar}>
        <span>{state.origin ? `Streaming from ${state.origin}` : ""}</span>
        {state.levels.length > 1 && (
          <label>
            Quality{" "}
            <select value={state.level} onChange={onQualityChange}>
              <option value={-1}>Auto</option>
              {state.levels.map((l) => (
                <option key={l.index} value={l.index}>
                  {l.label}
                </option>
              ))}
            </select>
          </label>
        )}
      </div>

      <p role="status" aria-live="polite" style={styles.notice}>
        {state.notice}
      </p>
    </main>
  );
}

const styles = {
  page: { maxWidth: 960, margin: "0 auto", padding: "24px 16px", fontFamily: "system-ui, sans-serif" },
  title: { fontSize: 22, margin: "0 0 16px" },
  stage: { position: "relative", background: "#000", borderRadius: 8, overflow: "hidden", minHeight: 200 },
  overlay: {
    position: "absolute",
    inset: 0,
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    justifyContent: "center",
    background: "rgba(0,0,0,0.65)",
    color: "#fff",
    textAlign: "center",
    padding: 16,
    zIndex: 5,
  },
  button: { padding: "8px 16px", borderRadius: 6, border: 0, cursor: "pointer", fontSize: 15 },
  bar: { display: "flex", justifyContent: "space-between", alignItems: "center", marginTop: 10, fontSize: 14, color: "#555" },
  notice: { minHeight: 20, marginTop: 8, fontSize: 14, color: "#8a5a00" },
};
