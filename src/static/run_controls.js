(() => {
  const newRunBtn = document.getElementById("new-run-btn");
  const cancelRunBtn = document.getElementById("cancel-run-btn");
  const runIdEl = document.getElementById("current-run-id");
  const statusEl = document.getElementById("current-run-status");
  const messageEl = document.getElementById("run-status-message");

  if (!newRunBtn || !cancelRunBtn || !runIdEl || !statusEl || !messageEl) {
    return;
  }

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

    if (status === "canceled") {
      statusEl.className = "fw-semibold text-danger";
    } else if (status) {
      statusEl.className = "text-secondary";
    } else {
      statusEl.className = "text-secondary";
    }

    cancelRunBtn.disabled = !currentRunId || status === "canceled";
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
})();

