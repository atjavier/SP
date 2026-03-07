(() => {
  const newRunBtn = document.getElementById("new-run-btn");
  const cancelRunBtn = document.getElementById("cancel-run-btn");
  const runIdEl = document.getElementById("current-run-id");
  const statusEl = document.getElementById("current-run-status");
  const referenceBuildEl = document.getElementById("current-run-reference-build");
  const stagesEl = document.getElementById("current-run-stages");
  const stagesMessageEl = document.getElementById("current-run-stages-message");
  const messageEl = document.getElementById("run-status-message");

  if (
    !newRunBtn ||
    !cancelRunBtn ||
    !runIdEl ||
    !statusEl ||
    !referenceBuildEl ||
    !stagesEl ||
    !stagesMessageEl ||
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
    if (status === "succeeded") return "Succeeded";
    if (status === "failed") return "Failed";
    if (status === "canceled") return "Canceled";
    return status;
  }

  function clearEl(el) {
    while (el.firstChild) {
      el.removeChild(el.firstChild);
    }
  }

  function setStagesMessage(text) {
    clearEl(stagesMessageEl);
    if (!text) return;
    const span = document.createElement("span");
    span.textContent = text;
    stagesMessageEl.appendChild(span);
  }

  function humanizeStageName(stageName) {
    const normalized = String(stageName || "").replace(/_/g, " ").trim();
    if (!normalized) return "Stage";
    return normalized.replace(/\b\w/g, (c) => c.toUpperCase());
  }

  function formatStageStatus(status) {
    if (!status) return "Queued";
    if (status === "queued") return "Queued";
    if (status === "running") return "Running";
    if (status === "succeeded") return "Succeeded";
    if (status === "failed") return "Failed";
    if (status === "canceled") return "Canceled";
    return String(status);
  }

  function stageBadgeClass(status) {
    if (status === "running") return "text-bg-primary";
    if (status === "succeeded") return "text-bg-success";
    if (status === "failed") return "text-bg-danger";
    if (status === "canceled") return "text-bg-dark";
    return "text-bg-secondary";
  }

  function renderStages(stages) {
    clearEl(stagesEl);
    if (!stages || stages.length === 0) {
      setStagesMessage("No stage status available.");
      return;
    }

    setStagesMessage("");

    for (const stage of stages) {
      const li = document.createElement("li");
      li.className = "list-group-item d-flex justify-content-between align-items-start";

      const body = document.createElement("div");
      body.className = "ms-2 me-auto";

      const title = document.createElement("div");
      title.className = "fw-semibold";
      title.textContent = humanizeStageName(stage?.stage_name);
      body.appendChild(title);

      const error = stage?.error ?? null;
      if (error?.code || error?.message) {
        const detail = document.createElement("div");
        detail.className = "small text-danger";
        const parts = [];
        if (error.code) parts.push(String(error.code));
        if (error.message) parts.push(String(error.message));
        detail.textContent = parts.join(": ");
        body.appendChild(detail);
      }

      const badge = document.createElement("span");
      const status = stage?.status ?? "queued";
      badge.className = `badge ${stageBadgeClass(status)} rounded-pill`;
      badge.textContent = formatStageStatus(status);

      li.appendChild(body);
      li.appendChild(badge);
      stagesEl.appendChild(li);
    }
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
    } else if (status === "failed") {
      statusEl.className = "fw-semibold text-danger";
    } else if (status === "succeeded") {
      statusEl.className = "fw-semibold text-success";
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

  async function refreshStagesFromServer(runId) {
    if (!runId) {
      renderStages(null);
      setStagesMessage("Create a run to see stage status.");
      return;
    }

    try {
      const { resp, payload } = await getJson(
        `/api/v1/runs/${encodeURIComponent(runId)}/stages`,
      );
      if (resp.ok && payload?.ok && payload?.data?.run_id === runId) {
        renderStages(payload.data.stages);
        return;
      }
      if (resp.status === 404) {
        renderStages(null);
        setStagesMessage("No run found.");
        return;
      }
    } catch {
      setStagesMessage("Unable to load stage status right now.");
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
      void refreshStagesFromServer(payload.data?.run_id);
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
      void refreshStagesFromServer(payload.data?.run_id);
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
      void refreshStagesFromServer(stored.run_id);
    } else {
      renderStages(null);
      setStagesMessage("Create a run to see stage status.");
    }
  } catch {
    // ignore storage failures
    renderStages(null);
    setStagesMessage("Create a run to see stage status.");
  }
})();
