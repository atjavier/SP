(() => {
  const newRunBtn = document.getElementById("new-run-btn");
  const cancelRunBtn = document.getElementById("cancel-run-btn");
  const runIdEl = document.getElementById("current-run-id");
  const statusEl = document.getElementById("current-run-status");
  const referenceBuildEl = document.getElementById("current-run-reference-build");
  const messageEl = document.getElementById("run-status-message");

  if (
    !newRunBtn ||
    !cancelRunBtn ||
    !runIdEl ||
    !statusEl ||
    !referenceBuildEl ||
    !messageEl
  ) {
    return;
  }

  const STORAGE_KEY = "sp_current_run";
  let currentRunId = null;

  function clearMessage() {
    while (messageEl.firstChild) {
      messageEl.removeChild(messageEl.firstChild);
    }
  }

  function setMessage(kind, text) {
    clearMessage();
    if (!text) return;

    const alertEl = document.createElement("div");
    alertEl.className =
      kind === "error"
        ? "alert alert-danger py-2 mb-0"
        : kind === "success"
          ? "alert alert-success py-2 mb-0"
          : "alert alert-info py-2 mb-0";
    alertEl.textContent = text;
    messageEl.appendChild(alertEl);
  }

  function formatStatus(status) {
    if (!status) return "No run";
    if (status === "queued") return "Queued";
    if (status === "running") return "Running";
    if (status === "canceled") return "Canceled";
    return status;
  }

  function setRun(run) {
    currentRunId = run?.run_id ?? null;
    runIdEl.textContent = currentRunId ?? "\u2014";

    const status = run?.status ?? null;
    statusEl.textContent = formatStatus(status);

    const referenceBuild = run?.reference_build ?? null;
    referenceBuildEl.textContent = referenceBuild ?? "\u2014";

    if (status === "canceled") {
      statusEl.className = "fw-semibold text-danger";
    } else if (status) {
      statusEl.className = "text-secondary";
    } else {
      statusEl.className = "text-secondary";
    }

    cancelRunBtn.disabled = !currentRunId || status === "canceled";

    try {
      if (currentRunId) {
        localStorage.setItem(
          STORAGE_KEY,
          JSON.stringify({
            run_id: currentRunId,
            status,
            created_at: run?.created_at ?? null,
            reference_build: referenceBuild,
          }),
        );
      } else {
        localStorage.removeItem(STORAGE_KEY);
      }
    } catch {
      // ignore storage failures
    }
  }

  async function postJson(url) {
    const resp = await fetch(url, {
      method: "POST",
      headers: { Accept: "application/json" },
    });
    const text = await resp.text();
    let payload = null;
    try {
      payload = JSON.parse(text);
    } catch {
      payload = null;
    }
    return { resp, payload };
  }

  async function getJson(url) {
    const resp = await fetch(url, {
      method: "GET",
      headers: { Accept: "application/json" },
    });
    const text = await resp.text();
    let payload = null;
    try {
      payload = JSON.parse(text);
    } catch {
      payload = null;
    }
    return { resp, payload };
  }

  async function refreshFromServer(runId) {
    try {
      const { resp, payload } = await getJson(
        `/api/v1/runs/${encodeURIComponent(runId)}`,
      );
      if (resp.ok && payload?.ok && payload?.data?.run_id === runId) {
        setRun(payload.data);
        return;
      }
      if (resp.status === 404) {
        setRun(null);
      }
    } catch {
      // ignore refresh failures
    }
  }

  newRunBtn.addEventListener("click", async () => {
    newRunBtn.disabled = true;
    setMessage(null, "");
    try {
      const { resp, payload } = await postJson("/api/v1/runs");
      if (!resp.ok || !payload?.ok) {
        const msg = payload?.error?.message ?? "Failed to create run.";
        setMessage("error", msg);
        return;
      }
      setRun(payload.data);
      setMessage("success", "Run created.");
    } catch {
      setMessage("error", "Failed to create run.");
    } finally {
      newRunBtn.disabled = false;
    }
  });

  cancelRunBtn.addEventListener("click", async () => {
    if (!currentRunId) return;
    cancelRunBtn.disabled = true;
    setMessage(null, "");
    try {
      const { resp, payload } = await postJson(
        `/api/v1/runs/${encodeURIComponent(currentRunId)}/cancel`,
      );
      if (!resp.ok || !payload?.ok) {
        const msg = payload?.error?.message ?? "Failed to cancel run.";
        setMessage("error", msg);
        cancelRunBtn.disabled = false;
        return;
      }
      setRun(payload.data);
      setMessage("info", "Run canceled.");
    } catch {
      setMessage("error", "Failed to cancel run.");
      cancelRunBtn.disabled = false;
    }
  });

  try {
    const stored = JSON.parse(localStorage.getItem(STORAGE_KEY));
    if (stored?.run_id) {
      setRun(stored);
      void refreshFromServer(stored.run_id);
    }
  } catch {
    // ignore storage failures
  }
})();
