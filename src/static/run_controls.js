(() => {
  const retryFailedStageBtn = document.getElementById("retry-failed-stage-btn");
  const cancelRunBtn = document.getElementById("cancel-run-btn");
  const evidencePolicyInputs = Array.from(
    document.querySelectorAll("input[name='annotation-evidence-policy']"),
  );
  const runIdEl = document.getElementById("current-run-id");
  const statusEl = document.getElementById("current-run-status");
  const referenceBuildEl = document.getElementById("current-run-reference-build");
  const evidencePolicyEl = document.getElementById("current-run-evidence-policy");
  const stagesEl = document.getElementById("current-run-stages");
  const stagesMessageEl = document.getElementById("current-run-stages-message");
  const messageEl = document.getElementById("run-status-message");
  const liveUpdatesEl = document.getElementById("live-updates-indicator");

  if (
    !cancelRunBtn ||
    !runIdEl ||
    !statusEl ||
    !referenceBuildEl ||
    !evidencePolicyEl ||
    !stagesEl ||
    !stagesMessageEl ||
    !messageEl ||
    !liveUpdatesEl
  ) {
    return;
  }

  const STORAGE_KEY = "sp_current_run";
  const PIPELINE_STAGE_ORDER = [
    "parser",
    "pre_annotation",
    "classification",
    "prediction",
    "annotation",
    "reporting",
  ];
  let currentRunId = null;
  let currentRunStatus = null;
  let currentRunEvidencePolicy = null;
  let lastStagesSnapshot = null;
  let eventSource = null;
  let elapsedTimerId = null;
  const dateTimeFormatter = new Intl.DateTimeFormat("en-US", {
    month: "2-digit",
    day: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });

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
    if (status === "queued") return "Idle";
    if (status === "running") return "Running";
    if (status === "succeeded") return "Succeeded";
    if (status === "failed") return "Failed";
    if (status === "canceled") return "Canceled";
    return status;
  }

  function formatEvidencePolicy(policy) {
    const normalized = String(policy || "").trim().toLowerCase();
    if (normalized === "stop") return "stop (fail annotation stage)";
    if (normalized === "continue") return "continue (allow partial evidence)";
    return "\u2014";
  }

  function selectedEvidencePolicy() {
    for (const input of evidencePolicyInputs) {
      if (input?.checked) return input.value;
    }
    return currentRunEvidencePolicy || "continue";
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

  function parseIsoDate(iso) {
    if (!iso) return null;
    const d = new Date(iso);
    if (Number.isNaN(d.getTime())) return null;
    return d;
  }

  function formatDateTime(iso) {
    if (!iso) return "\u2014";
    const d = parseIsoDate(iso);
    if (!d) return String(iso);
    return dateTimeFormatter.format(d);
  }

  function formatElapsed(ms) {
    if (ms == null || !Number.isFinite(ms) || ms < 0) return "\u2014";
    const totalSeconds = Math.floor(ms / 1000);
    const seconds = totalSeconds % 60;
    const minutes = Math.floor(totalSeconds / 60) % 60;
    const hours = Math.floor(totalSeconds / 3600);
    if (hours > 0) return `${hours}h ${minutes}m ${seconds}s`;
    if (minutes > 0) return `${minutes}m ${seconds}s`;
    return `${seconds}s`;
  }

  function stopElapsedTimer() {
    if (elapsedTimerId == null) return;
    window.clearInterval(elapsedTimerId);
    elapsedTimerId = null;
  }

  function ensureStageRow(stageName) {
    for (const li of stagesEl.querySelectorAll("li[data-stage-name]")) {
      if (li.dataset.stageName === stageName) return li;
    }

    const li = document.createElement("li");
    li.className = "list-group-item d-flex justify-content-between align-items-start";
    li.dataset.stageName = stageName;

    const body = document.createElement("div");
    body.className = "ms-2 me-auto";

    const title = document.createElement("div");
    title.className = "fw-semibold";
    title.dataset.role = "stage-title";
    body.appendChild(title);

    const detail = document.createElement("div");
    detail.className = "small text-danger";
    detail.dataset.role = "stage-error";
    body.appendChild(detail);

    const statsLine = document.createElement("div");
    statsLine.className = "small text-secondary";
    statsLine.dataset.role = "stage-stats";
    body.appendChild(statsLine);

    const timing = document.createElement("div");
    timing.className = "small text-secondary mt-1";
    timing.dataset.role = "stage-timing";

    timing.appendChild(document.createTextNode("Started: "));
    const startedCode = document.createElement("code");
    startedCode.dataset.role = "stage-started-at";
    startedCode.textContent = "\u2014";
    timing.appendChild(startedCode);

    timing.appendChild(document.createTextNode(" Completed: "));
    const completedCode = document.createElement("code");
    completedCode.dataset.role = "stage-completed-at";
    completedCode.textContent = "\u2014";
    timing.appendChild(completedCode);

    timing.appendChild(document.createTextNode(" Elapsed: "));
    const elapsedSpan = document.createElement("span");
    elapsedSpan.dataset.role = "stage-elapsed";
    elapsedSpan.textContent = "\u2014";
    timing.appendChild(elapsedSpan);
    body.appendChild(timing);

    const badge = document.createElement("span");
    badge.className = "badge text-bg-secondary rounded-pill";
    badge.dataset.role = "stage-badge";

    li.appendChild(body);
    li.appendChild(badge);
    stagesEl.appendChild(li);
    return li;
  }

  function basename(path) {
    if (!path) return "";
    const text = String(path);
    const parts = text.split(/[/\\\\]+/);
    return parts[parts.length - 1] || text;
  }

  function sanitizeInline(text, maxLen = 240) {
    if (!text) return "";
    const compact = String(text).replace(/\s+/g, " ").trim();
    if (compact.length <= maxLen) return compact;
    return `${compact.slice(0, maxLen)}\u2026`;
  }

  function renderStageStats(stageName, stage, statsEl) {
    if (!statsEl) return;
    clearEl(statsEl);

    const stats = stage?.stats ?? null;
    const error = stage?.error ?? null;

    const normalizedName = String(stageName || "");
    const tool = stats && typeof stats === "object" ? stats?.tool ?? null : null;
    const note = stats && typeof stats === "object" ? stats?.note ?? null : null;

    let text = "";
    if (
      normalizedName === "annotation" &&
      error?.code === "SNPEFF_NOT_CONFIGURED" &&
      error?.details?.hint
    ) {
      text = String(error.details.hint);
    } else if (
      normalizedName === "annotation" &&
      error?.code === "SNPEFF_DATADIR_INVALID" &&
      error?.details?.hint
    ) {
      text = String(error.details.hint);
    } else if (
      normalizedName === "annotation" &&
      error?.code === "SNPEFF_DB_MISSING" &&
      error?.details?.expected_db_file
    ) {
      text = `Missing DB: ${sanitizeInline(error.details.expected_db_file, 180)}`;
    } else if (
      normalizedName === "annotation" &&
      error?.code === "SNPEFF_FAILED" &&
      error?.details?.stderr_tail
    ) {
      text = `stderr: ${sanitizeInline(error.details.stderr_tail, 220)}`;
    } else if (normalizedName === "parser" && stats?.snv_records_persisted != null) {
      text = `SNVs persisted: ${stats.snv_records_persisted}`;
    } else if (
      normalizedName === "pre_annotation" &&
      stats?.variants_processed != null
    ) {
      text = `Variants processed: ${stats.variants_processed}`;
    } else if (
      normalizedName === "classification" &&
      stats?.variants_processed != null
    ) {
      const counts = stats?.category_counts;
      if (counts && typeof counts === "object") {
        const parts = [];
        for (const key of ["missense", "synonymous", "nonsense", "other", "unclassified"]) {
          const value = counts?.[key];
          if (value != null) parts.push(`${key}: ${value}`);
        }
        text = parts.length > 0
          ? `Variants processed: ${stats.variants_processed}. ${parts.join(", ")}.`
          : `Variants processed: ${stats.variants_processed}`;
      } else {
        text = `Variants processed: ${stats.variants_processed}`;
      }
    } else if (normalizedName === "prediction" && stats?.variants_processed != null) {
      text = `Variants processed: ${stats.variants_processed}`;
    } else if (normalizedName === "annotation" && tool === "snpeff") {
      if (note) {
        text = String(note);
      } else if (stats?.output_vcf_path) {
        text = `SnpEff output: ${basename(stats.output_vcf_path)}`;
      } else {
        text = "SnpEff completed.";
      }
    } else if (note) {
      text = String(note);
    }

    if (!text) {
      statsEl.style.display = "none";
      return;
    }

    const span = document.createElement("span");
    span.textContent = text;
    statsEl.appendChild(span);
    statsEl.style.display = "";
  }

  function updateElapsedForRow(li) {
    const startedAt = li.dataset.startedAt || null;
    const completedAt = li.dataset.completedAt || null;
    const status = li.dataset.stageStatus || "queued";

    const started = parseIsoDate(startedAt);
    const completed = parseIsoDate(completedAt);

    let elapsedMs = null;
    if (started && completed) {
      elapsedMs = completed.getTime() - started.getTime();
    } else if (started && status === "running") {
      elapsedMs = Date.now() - started.getTime();
    }

    const elapsedEl = li.querySelector('[data-role="stage-elapsed"]');
    if (elapsedEl) elapsedEl.textContent = formatElapsed(elapsedMs);
  }

  function updateAllElapsed() {
    for (const li of stagesEl.querySelectorAll("li[data-stage-name]")) {
      updateElapsedForRow(li);
    }
  }

  function ensureElapsedTimer() {
    if (elapsedTimerId != null) return;
    elapsedTimerId = window.setInterval(() => {
      updateAllElapsed();
    }, 1000);
  }

  function firstFailedStageName(stages) {
    if (!stages || stages.length === 0) return null;
    const byName = new Map();
    for (const stage of stages) {
      const name = String(stage?.stage_name || "");
      if (name) byName.set(name, stage);
    }

    for (const stageName of PIPELINE_STAGE_ORDER) {
      const stage = byName.get(stageName);
      if (stage?.status === "failed") return stageName;
    }

    for (const stage of stages) {
      if (stage?.status === "failed" && stage?.stage_name) {
        return String(stage.stage_name);
      }
    }

    return null;
  }

  function isTerminalPipelineSnapshot(stages) {
    if (!Array.isArray(stages) || stages.length === 0) return false;
    const hasRunning = stages.some((stage) => stage?.status === "running");
    if (hasRunning) return false;
    const hasFailedOrCanceled = stages.some((stage) => {
      const status = stage?.status;
      return status === "failed" || status === "canceled";
    });
    if (hasFailedOrCanceled) return true;
    for (const stageName of PIPELINE_STAGE_ORDER) {
      if (stageName !== "reporting") continue;
      const stage = stages.find((entry) => (entry?.stage_name ?? null) === stageName);
      return stage?.status === "succeeded";
    }
    return false;
  }

  function updateRetryControl(stages) {
    if (!retryFailedStageBtn) return;

    const failedStage = firstFailedStageName(stages);
    const anyRunning =
      Array.isArray(stages) && stages.some((s) => (s?.status ?? null) === "running");

    if (
      !currentRunId ||
      !failedStage ||
      anyRunning ||
      currentRunStatus === "canceled" ||
      currentRunStatus === "running"
    ) {
      retryFailedStageBtn.hidden = true;
      retryFailedStageBtn.disabled = true;
      retryFailedStageBtn.dataset.stageName = "";
      return;
    }

    retryFailedStageBtn.hidden = false;
    retryFailedStageBtn.disabled = false;
    retryFailedStageBtn.dataset.stageName = failedStage;
  }

  function renderStages(stages) {
    if (!stages || stages.length === 0) {
      lastStagesSnapshot = null;
      clearEl(stagesEl);
      stopElapsedTimer();
      setStagesMessage("No stage status available.");
      updateRetryControl(null);
      return;
    }

    lastStagesSnapshot = stages;
    setStagesMessage("");
    const snapshotStageNames = new Set(
      stages.map((s) => String(s?.stage_name || "")).filter((name) => Boolean(name)),
    );
    for (const li of stagesEl.querySelectorAll("li[data-stage-name]")) {
      if (!snapshotStageNames.has(li.dataset.stageName)) {
        li.remove();
      }
    }

    let anyRunningForElapsed = false;
    for (const stage of stages) {
      const stageName = String(stage?.stage_name || "");
      if (!stageName) continue;

      const li = ensureStageRow(stageName);
      stagesEl.appendChild(li);

      const titleEl = li.querySelector('[data-role="stage-title"]');
      if (titleEl) titleEl.textContent = humanizeStageName(stageName);

      const error = stage?.error ?? null;
      const errorEl = li.querySelector('[data-role="stage-error"]');
      if (errorEl) {
        if (error?.code || error?.message) {
          const parts = [];
          if (error.code) parts.push(String(error.code));
          if (error.message) parts.push(String(error.message));
          if (
            stageName === "annotation" &&
            error?.code === "SNPEFF_FAILED" &&
            error?.details?.exit_code != null
          ) {
            parts.push(`exit_code=${error.details.exit_code}`);
          }
          if (stageName === "annotation" && error?.details?.failed_source) {
            parts.push(`failed_source=${error.details.failed_source}`);
          }
          errorEl.textContent = parts.join(": ");
          errorEl.style.display = "";
        } else {
          errorEl.textContent = "";
          errorEl.style.display = "none";
        }
      }

      const statsEl = li.querySelector('[data-role="stage-stats"]');
      renderStageStats(stageName, stage ?? null, statsEl);

      const status = stage?.status ?? "queued";
      li.dataset.stageStatus = status;
      li.dataset.startedAt = stage?.started_at ?? "";
      li.dataset.completedAt = stage?.completed_at ?? "";

      if (status === "running" && li.dataset.startedAt && !li.dataset.completedAt) {
        anyRunningForElapsed = true;
      }

      const startedEl = li.querySelector('[data-role="stage-started-at"]');
      if (startedEl) startedEl.textContent = formatDateTime(stage?.started_at ?? null);
      const completedEl = li.querySelector('[data-role="stage-completed-at"]');
      if (completedEl) completedEl.textContent = formatDateTime(stage?.completed_at ?? null);

      const badgeEl = li.querySelector('[data-role="stage-badge"]');
      if (badgeEl) {
        badgeEl.className = `badge ${stageBadgeClass(status)} rounded-pill`;
        badgeEl.textContent = formatStageStatus(status);
      }

      updateElapsedForRow(li);
    }

    if (anyRunningForElapsed) {
      ensureElapsedTimer();
    } else {
      stopElapsedTimer();
    }

    updateRetryControl(stages);

    if (isTerminalPipelineSnapshot(stages)) {
      closeEventSource();
      setLiveUpdates("success", "Live updates: completed.");
    }
  }

  function setLiveUpdates(kind, text) {
    clearEl(liveUpdatesEl);
    const span = document.createElement("span");
    span.className =
      kind === "error"
        ? "text-danger"
        : kind === "warning"
          ? "text-warning"
          : kind === "success"
            ? "text-success"
            : "text-secondary";
    span.textContent = text || "Not connected.";
    liveUpdatesEl.appendChild(span);
  }

  function dispatchRunChanged(run) {
    try {
      window.dispatchEvent(
        new CustomEvent("sp:run-changed", {
          detail: {
            run: run || null,
            source: "run-controls",
          },
        }),
      );
    } catch {
      // ignore event failures
    }
  }

  function closeEventSource() {
    if (eventSource) {
      try {
        eventSource.close();
      } catch {
        // ignore
      }
      eventSource = null;
    }
  }

  function resetTaskQueueState() {
    closeEventSource();
    stopElapsedTimer();
    currentRunId = null;
    currentRunStatus = null;
    currentRunEvidencePolicy = null;
    lastStagesSnapshot = null;
    runIdEl.textContent = "\u2014";
    statusEl.textContent = formatStatus(null);
    referenceBuildEl.textContent = "\u2014";
    evidencePolicyEl.textContent = "\u2014";
    updateCancelVisibility(null);
    renderStages(null);
    setStagesMessage("Choose a VCF file and press Start.");
    setLiveUpdates(null, "Not connected.");
    clearMessage();
  }

  async function reconcileAfterReconnect(runId) {
    await refreshFromServer(runId);
    await refreshStagesFromServer(runId);
  }

  function ensureEventSource(runId) {
    if (!runId) {
      closeEventSource();
      setLiveUpdates(null, "Not connected.");
      return;
    }

    if (eventSource && eventSource.__runId === runId) return;

    closeEventSource();
    setLiveUpdates("warning", "Live updates: connecting...");

    const url = `/api/v1/runs/${encodeURIComponent(runId)}/events`;
    eventSource = new EventSource(url);
    eventSource.__runId = runId;

    eventSource.onopen = () => {
      setLiveUpdates("success", "Live updates: connected.");
      void reconcileAfterReconnect(runId);
    };

    eventSource.onerror = () => {
      setLiveUpdates("warning", "Live updates paused. Reconnecting...");
    };

    eventSource.addEventListener("run_status", (ev) => {
      try {
        const parsed = JSON.parse(ev.data);
        if (parsed?.run_id !== runId) return;
        const status = parsed?.data?.status ?? null;
        setRun({ run_id: runId, status, reference_build: referenceBuildEl.textContent });
      } catch {
        // ignore invalid events
      }
    });

    eventSource.addEventListener("stage_status", (ev) => {
      try {
        const parsed = JSON.parse(ev.data);
        if (parsed?.run_id !== runId) return;
        // simplest and safest: refetch ordered stages snapshot
        void refreshStagesFromServer(runId);
      } catch {
        // ignore invalid events
      }
    });
  }

  function setRun(run) {
    currentRunId = run?.run_id ?? null;
    runIdEl.textContent = currentRunId ?? "\u2014";

    const status = run?.status ?? null;
    currentRunStatus = status;
    statusEl.textContent = formatStatus(status);

    const referenceBuild = run?.reference_build ?? null;
    referenceBuildEl.textContent = referenceBuild ?? "\u2014";
    const evidencePolicy = run?.annotation_evidence_policy ?? currentRunEvidencePolicy;
    currentRunEvidencePolicy = evidencePolicy ?? null;
    evidencePolicyEl.textContent = formatEvidencePolicy(evidencePolicy);

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

    updateCancelVisibility(status);
    updateRetryControl(lastStagesSnapshot);
    ensureEventSource(currentRunId);

    try {
      if (currentRunId) {
        localStorage.setItem(
          STORAGE_KEY,
          JSON.stringify({
            run_id: currentRunId,
            status,
            created_at: run?.created_at ?? null,
            reference_build: referenceBuild,
            annotation_evidence_policy: evidencePolicy ?? null,
          }),
        );
      } else {
        localStorage.removeItem(STORAGE_KEY);
      }
    } catch {
      // ignore storage failures
    }

    dispatchRunChanged(run);
  }

  async function postJson(url, body = null) {
    const hasBody = body != null;
    const resp = await fetch(url, {
      method: "POST",
      headers: hasBody
        ? { Accept: "application/json", "Content-Type": "application/json" }
        : { Accept: "application/json" },
      body: hasBody ? JSON.stringify(body) : undefined,
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

  function updateCancelVisibility(status) {
    const normalized = status || null;
    const hasRun = Boolean(currentRunId);
    const isRunning = normalized === "running";
    cancelRunBtn.hidden = !hasRun || !isRunning;
    cancelRunBtn.disabled = !hasRun || !isRunning;
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
      setStagesMessage("Choose a VCF file and press Start.");
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

  if (retryFailedStageBtn) {
    retryFailedStageBtn.addEventListener("click", async () => {
      if (!currentRunId) return;
      const stageName = String(retryFailedStageBtn.dataset.stageName || "").trim();
      if (!stageName) return;

      retryFailedStageBtn.disabled = true;
      setMessage(null, "");
      try {
        const requestedPolicy = selectedEvidencePolicy();
        const { resp: settingsResp, payload: settingsPayload } = await postJson(
          `/api/v1/runs/${encodeURIComponent(currentRunId)}/settings`,
          { annotation_evidence_policy: requestedPolicy },
        );
        if (!settingsResp.ok || !settingsPayload?.ok) {
          const msg = settingsPayload?.error?.message ?? "Failed to update run settings.";
          setMessage("error", msg);
          return;
        }
        if (settingsPayload?.data) {
          setRun(settingsPayload.data);
        }

        const { resp, payload } = await postJson(
          `/api/v1/runs/${encodeURIComponent(currentRunId)}/stages/${encodeURIComponent(stageName)}/retry`,
        );
        if (!resp.ok || !payload?.ok) {
          const msg = payload?.error?.message ?? "Failed to retry stage.";
          setMessage("error", msg);
          return;
        }

        const preserved = Array.isArray(payload?.data?.preserved_stages)
          ? payload.data.preserved_stages
          : [];
        const reset = Array.isArray(payload?.data?.reset_stages) ? payload.data.reset_stages : [];

        const preservedText =
          preserved.length > 0 ? preserved.map(humanizeStageName).join(", ") : "None";
        const resetText = reset.length > 0 ? reset.map(humanizeStageName).join(", ") : "None";

        setMessage(
          "info",
          `Retrying from ${humanizeStageName(stageName)}. Preserved: ${preservedText}. Re-running: ${resetText}.`,
        );

        void refreshFromServer(currentRunId);
        void refreshStagesFromServer(currentRunId);
      } catch {
        setMessage("error", "Failed to retry stage.");
      } finally {
        retryFailedStageBtn.disabled = false;
      }
    });
  }

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

  window.addEventListener("sp:run-changed", (evt) => {
    const detail = evt?.detail ?? null;
    if (detail?.source === "run-controls") {
      return;
    }
    const run = detail?.run ?? detail ?? null;
    if (!run?.run_id) {
      setRun(null);
      renderStages(null);
      setStagesMessage("Choose a VCF file and press Start.");
      setLiveUpdates(null, "Not connected.");
      clearMessage();
      return;
    }
    setRun(run);
    void refreshFromServer(run.run_id);
    void refreshStagesFromServer(run.run_id);
  });

  window.addEventListener("sp:task-queue-reset", () => {
    resetTaskQueueState();
  });

  try {
    const stored = JSON.parse(localStorage.getItem(STORAGE_KEY));
    if (stored?.run_id) {
      setRun(stored);
      void refreshFromServer(stored.run_id);
      void refreshStagesFromServer(stored.run_id);
    } else {
      renderStages(null);
      setStagesMessage("Choose a VCF file and press Start.");
      setLiveUpdates(null, "");
    }
  } catch {
    // ignore storage failures
    renderStages(null);
    setStagesMessage("Choose a VCF file and press Start.");
    setLiveUpdates(null, "");
  }
})();
