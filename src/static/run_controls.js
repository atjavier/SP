(() => {
  const retryFailedStageBtn = document.getElementById("retry-failed-stage-btn");
  const cancelRunBtn = document.getElementById("cancel-run-btn");
  const evidencePolicyInputs = Array.from(
    document.querySelectorAll("input[name='annotation-evidence-policy']"),
  );
  const runIdEl = document.getElementById("current-run-id");
  const statusEl = document.getElementById("current-run-status");
  const referenceBuildEl = document.getElementById("current-run-reference-build");
  const stagesEl = document.getElementById("current-run-stages");
  const stagesMessageEl = document.getElementById("current-run-stages-message");
  const runLogsPanelEl = document.getElementById("run-logs-panel");
  const runLogsConsoleEl = document.getElementById("run-logs-console");
  const runLogsMessageEl = document.getElementById("run-logs-message");
  const messageEl = document.getElementById("run-status-message");
  const liveUpdatesEl = document.getElementById("live-updates-indicator");
  const workspaceTabsEl = document.getElementById("workspace-tabs");

  if (
    !cancelRunBtn ||
    !runIdEl ||
    !statusEl ||
    !referenceBuildEl ||
    !stagesEl ||
    !stagesMessageEl ||
    !messageEl ||
    !liveUpdatesEl
  ) {
    return;
  }

  const logsEnabled = Boolean(runLogsPanelEl && runLogsConsoleEl && runLogsMessageEl);
  const STORAGE_KEY = "sp_current_run";
  const RUN_LOG_POLL_MS = 3000;
  const RUN_LOG_LIMIT = 200;
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
  let currentRunEvidenceModeRequested = null;
  let currentRunEvidenceModeEffective = null;
  let currentRunEvidenceModeReason = null;
  let lastStagesSnapshot = null;
  let eventSource = null;
  let elapsedTimerId = null;
  let runLogPollId = null;
  let lastLogRunId = null;
  let runLogsLoaded = false;
  const dateTimeFormatter = new Intl.DateTimeFormat("en-US", {
    month: "2-digit",
    day: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });

  function activateWorkspaceTab(tabEl) {
    if (!tabEl) return;
    if (window.bootstrap?.Tab) {
      window.bootstrap.Tab.getOrCreateInstance(tabEl).show();
      return;
    }
    tabEl.click();
  }

  function getWorkspaceTabs() {
    if (!workspaceTabsEl) return [];
    return Array.from(workspaceTabsEl.querySelectorAll('[role="tab"]'));
  }

  function moveWorkspaceTabFocus(tabs, nextIndex, activate) {
    if (!tabs.length) return;
    let targetIndex = nextIndex;
    if (targetIndex < 0) targetIndex = tabs.length - 1;
    if (targetIndex >= tabs.length) targetIndex = 0;
    const target = tabs[targetIndex];
    if (!target) return;
    target.focus();
    if (activate) activateWorkspaceTab(target);
  }

  function handleWorkspaceTabKeydown(event) {
    if (!workspaceTabsEl) return;
    const tabEl = event.target?.closest?.('[role="tab"]');
    if (!tabEl || !workspaceTabsEl.contains(tabEl)) return;
    const tabs = getWorkspaceTabs();
    const currentIndex = tabs.indexOf(tabEl);
    if (currentIndex < 0) return;

    let nextIndex = null;
    switch (event.key) {
      case "ArrowLeft":
        nextIndex = currentIndex - 1;
        break;
      case "ArrowRight":
        nextIndex = currentIndex + 1;
        break;
      case "Home":
        nextIndex = 0;
        break;
      case "End":
        nextIndex = tabs.length - 1;
        break;
      case "Enter":
      case " ":
        event.preventDefault();
        activateWorkspaceTab(tabEl);
        return;
      default:
        return;
    }

    if (nextIndex != null) {
      event.preventDefault();
      moveWorkspaceTabFocus(tabs, nextIndex, false);
    }
  }

  function clearMessage() {
    while (messageEl.firstChild) {
      messageEl.removeChild(messageEl.firstChild);
    }
  }

  function clearRunLogsMessage() {
    if (!logsEnabled) return;
    while (runLogsMessageEl.firstChild) {
      runLogsMessageEl.removeChild(runLogsMessageEl.firstChild);
    }
  }

  function setRunLogsMessage(text) {
    if (!logsEnabled) return;
    const normalized = text || "";
    if (runLogsMessageEl.textContent === normalized) return;
    clearRunLogsMessage();
    if (!normalized) return;
    const span = document.createElement("span");
    span.textContent = normalized;
    runLogsMessageEl.appendChild(span);
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

  const STATUS_LABEL_OVERRIDES = {
    queued: "Queued",
    running: "Running",
    succeeded: "Succeeded",
    failed: "Failed",
    canceled: "Canceled",
  };
  const STATUS_ICON_MAP = {
    queued: "[~]",
    running: "[~]",
    succeeded: "[OK]",
    failed: "[!]",
    canceled: "[x]",
  };

  function normalizeStatusKey(value) {
    if (!value) return "";
    return String(value).trim().toLowerCase().replace(/_/g, " ");
  }

  function statusLabel(status) {
    const normalized = normalizeStatusKey(status);
    if (!normalized) return "";
    if (STATUS_LABEL_OVERRIDES[normalized]) return STATUS_LABEL_OVERRIDES[normalized];
    return normalized.replace(/\b\w/g, (c) => c.toUpperCase());
  }

  function statusIcon(status) {
    const normalized = normalizeStatusKey(status);
    return STATUS_ICON_MAP[normalized] || "";
  }

  function buildStatusIndicator(status, labelText) {
    const indicator = document.createElement("span");
    indicator.className = "status-indicator";
    const normalized = normalizeStatusKey(status);
    if (normalized) {
      indicator.dataset.status = normalized.replace(/\s+/g, "-");
    }

    const icon = statusIcon(status);
    if (icon) {
      const iconSpan = document.createElement("span");
      iconSpan.className = "status-icon";
      iconSpan.setAttribute("aria-hidden", "true");
      iconSpan.textContent = icon;
      indicator.appendChild(iconSpan);
    }

    const label = labelText || statusLabel(status) || (status ? String(status) : "\u2014");
    const labelSpan = document.createElement("span");
    labelSpan.className = "status-label";
    labelSpan.textContent = label;
    indicator.appendChild(labelSpan);

    return indicator;
  }

  function setStatusIndicator(container, status, labelText) {
    if (!container) return;
    clearEl(container);
    container.appendChild(buildStatusIndicator(status, labelText));
  }

  function formatStatus(status) {
    if (!status) return "No run";
    return statusLabel(status) || status;
  }

  function formatEvidenceMode(mode) {
    const normalized = String(mode || "").trim().toLowerCase();
    if (normalized === "online") return "online";
    if (normalized === "offline") return "offline";
    if (normalized === "hybrid") return "hybrid";
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
    const normalized = String(stageName || "").trim();
    if (!normalized) return "Stage";
    const overrides = {
      classification: "Consequence classification (VEP)",
      prediction: "Functional prediction",
      annotation: "Evidence annotation",
    };
    if (overrides[normalized]) return overrides[normalized];
    const spaced = normalized.replace(/_/g, " ").trim();
    if (!spaced) return "Stage";
    return spaced.replace(/\b\w/g, (c) => c.toUpperCase());
  }

  function formatStageStatus(status) {
    if (!status) return "Queued";
    return statusLabel(status) || String(status);
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

  function isPipelineActive(status) {
    return status === "running";
  }

  function isScrolledToBottom(el) {
    if (!el) return true;
    const threshold = 4;
    return el.scrollTop + el.clientHeight >= el.scrollHeight - threshold;
  }

  function formatLogLine(entry) {
    if (!entry || typeof entry !== "object") {
      return sanitizeInline(entry);
    }
    const eventAt = entry.event_at ? String(entry.event_at) : "\u2014";
    const level = entry.level ? String(entry.level).toUpperCase() : "INFO";
    const eventName = entry.event ? String(entry.event) : "event";
    const stageName = entry.stage_name ? ` stage=${entry.stage_name}` : "";
    const status = entry.status ? ` status=${entry.status}` : "";
    const message = entry.message ? ` - ${sanitizeInline(entry.message, 200)}` : "";
    const errorCode = entry.error_code ? ` error=${entry.error_code}` : "";
    const errorMessage = entry.error_message
      ? ` reason=${sanitizeInline(entry.error_message, 200)}`
      : "";
    return `${eventAt} [${level}] ${eventName}${stageName}${status}${errorCode}${errorMessage}${message}`;
  }

  function renderRunLogs(logs) {
    if (!logsEnabled) return;
    const entries = Array.isArray(logs) ? logs : [];
    if (entries.length === 0) {
      runLogsConsoleEl.textContent = "";
      setRunLogsMessage("No log lines yet.");
      return;
    }

    const shouldStickToBottom = isScrolledToBottom(runLogsPanelEl);
    const previousScrollTop = runLogsPanelEl.scrollTop;
    runLogsConsoleEl.textContent = entries.map(formatLogLine).join("\n");
    setRunLogsMessage("");

    if (shouldStickToBottom) {
      runLogsPanelEl.scrollTop = runLogsPanelEl.scrollHeight;
    } else {
      runLogsPanelEl.scrollTop = Math.min(previousScrollTop, runLogsPanelEl.scrollHeight);
    }
  }

  async function refreshRunLogs(runId) {
    if (!logsEnabled || !runId) return;
    if (!runLogsLoaded) {
      setRunLogsMessage("Loading log lines...");
    }
    try {
      const { resp, payload } = await getJson(
        `/api/v1/runs/${encodeURIComponent(runId)}/logs?limit=${RUN_LOG_LIMIT}`,
      );
      if (resp.ok && payload?.ok && payload?.data?.run_id === runId) {
        renderRunLogs(payload.data.logs ?? []);
        runLogsLoaded = true;
        return;
      }
      if (resp.status === 404) {
        runLogsConsoleEl.textContent = "";
        setRunLogsMessage("No run found.");
        runLogsLoaded = true;
        return;
      }
      runLogsConsoleEl.textContent = "";
      setRunLogsMessage("Unable to load logs right now.");
      runLogsLoaded = true;
    } catch {
      runLogsConsoleEl.textContent = "";
      setRunLogsMessage("Unable to load logs right now.");
      runLogsLoaded = true;
    }
  }

  function stopRunLogPolling() {
    if (runLogPollId == null) return;
    window.clearInterval(runLogPollId);
    runLogPollId = null;
  }

  function updateRunLogPolling() {
    if (!logsEnabled) return;
    if (!currentRunId) {
      stopRunLogPolling();
      lastLogRunId = null;
      runLogsLoaded = false;
      runLogsConsoleEl.textContent = "";
      setRunLogsMessage("Start a run to view recent log lines.");
      return;
    }

    const shouldPoll = isPipelineActive(currentRunStatus);
    if (shouldPoll) {
      if (runLogPollId == null || lastLogRunId !== currentRunId) {
        stopRunLogPolling();
        lastLogRunId = currentRunId;
        runLogsLoaded = false;
        void refreshRunLogs(currentRunId);
        runLogPollId = window.setInterval(() => {
          void refreshRunLogs(currentRunId);
        }, RUN_LOG_POLL_MS);
      }
      return;
    }

    stopRunLogPolling();
    lastLogRunId = currentRunId;
    runLogsLoaded = false;
    void refreshRunLogs(currentRunId);
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
    const effectiveMode = stats && typeof stats === "object" ? stats?.evidence_mode_effective ?? stats?.evidence_mode ?? null : null;
    const requestedMode = stats && typeof stats === "object" ? stats?.evidence_mode_requested ?? null : null;
    const modeSnippet =
      effectiveMode || requestedMode
        ? `Evidence mode: requested=${formatEvidenceMode(requestedMode)} effective=${formatEvidenceMode(effectiveMode)}`
        : "";

    let text = "";
    if (
      normalizedName === "annotation" &&
      error?.code === "SNPEFF_NOT_CONFIGURED" &&
      error?.details?.hint
    ) {
      text = String(error.details.hint);
    } else if (
      normalizedName === "annotation" &&
      error?.code === "EVIDENCE_SOURCES_UNAVAILABLE"
    ) {
      const details =
        error?.details && typeof error.details === "object" ? error.details : {};
      const missingSources = Array.isArray(details?.missing_sources)
        ? details.missing_sources.filter((value) => typeof value === "string" && value.trim())
        : [];
      const blockedOutputs = Array.isArray(details?.blocked_outputs)
        ? details.blocked_outputs.filter((value) => typeof value === "string" && value.trim())
        : [];
      const parts = [];
      if (missingSources.length > 0) {
        parts.push(`Missing evidence sources: ${missingSources.join(", ")}`);
      }
      if (blockedOutputs.length > 0) {
        parts.push(`Blocked outputs: ${blockedOutputs.join(", ")}`);
      }
      if (details?.hint) {
        parts.push(String(details.hint));
      }
      text = parts.join(". ");
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
    } else if (normalizedName === "reporting" && modeSnippet) {
      text = modeSnippet;
    } else if (note) {
      text = String(note);
    }

    if (modeSnippet && normalizedName === "annotation") {
      text = text ? `${text} ${modeSnippet}` : modeSnippet;
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
        setStatusIndicator(badgeEl, status, formatStageStatus(status));
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
    stopRunLogPolling();
    currentRunId = null;
    currentRunStatus = null;
    currentRunEvidencePolicy = null;
    currentRunEvidenceModeRequested = null;
    currentRunEvidenceModeEffective = null;
    currentRunEvidenceModeReason = null;
    lastStagesSnapshot = null;
    runIdEl.textContent = "\u2014";
    setStatusIndicator(statusEl, null, formatStatus(null));
    referenceBuildEl.textContent = "\u2014";
    updateCancelVisibility(null);
    renderStages(null);
    setStagesMessage("Choose a VCF file and press Start.");
    setLiveUpdates(null, "Not connected.");
    clearMessage();
    if (logsEnabled) {
      runLogsLoaded = false;
      runLogsConsoleEl.textContent = "";
      setRunLogsMessage("Start a run to view recent log lines.");
    }
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
        setRun({
          run_id: runId,
          status,
          reference_build: referenceBuildEl.textContent,
          annotation_evidence_policy: currentRunEvidencePolicy,
          evidence_mode_requested: currentRunEvidenceModeRequested,
          evidence_mode_effective: currentRunEvidenceModeEffective,
          evidence_mode_decision_reason: currentRunEvidenceModeReason,
        });
      } catch {
        // ignore invalid events
      }
    });

    eventSource.addEventListener("stage_status", (ev) => {
      try {
        const parsed = JSON.parse(ev.data);
        if (parsed?.run_id !== runId) return;
        // Keep stage list and run-level metadata in sync when annotation
        // preflight updates effective evidence mode telemetry.
        void refreshStagesFromServer(runId);
        void refreshFromServer(runId);
      } catch {
        // ignore invalid events
      }
    });

    eventSource.addEventListener("variant_result", (ev) => {
      try {
        const parsed = JSON.parse(ev.data);
        if (parsed?.run_id !== runId) return;
        const data = parsed?.data ?? {};
        window.dispatchEvent(
          new CustomEvent("sp:variant-result", {
            detail: {
              run_id: runId,
              stage_name: data?.stage_name ?? null,
              status: data?.status ?? null,
              variants_written: data?.variants_written ?? null,
            },
          }),
        );
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
    setStatusIndicator(statusEl, status, formatStatus(status));

    const referenceBuild = run?.reference_build ?? null;
    referenceBuildEl.textContent = referenceBuild ?? "\u2014";
    const evidencePolicy = run?.annotation_evidence_policy ?? currentRunEvidencePolicy;
    currentRunEvidencePolicy = evidencePolicy ?? null;
    const evidenceModeRequested = run?.evidence_mode_requested ?? currentRunEvidenceModeRequested;
    const evidenceModeEffective = run?.evidence_mode_effective ?? currentRunEvidenceModeEffective;
    const evidenceModeReason = run?.evidence_mode_decision_reason ?? currentRunEvidenceModeReason;
    currentRunEvidenceModeRequested = evidenceModeRequested ?? null;
    currentRunEvidenceModeEffective = evidenceModeEffective ?? null;
    currentRunEvidenceModeReason = evidenceModeReason ?? null;

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
    updateRunLogPolling();

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
            evidence_mode_requested: evidenceModeRequested ?? null,
            evidence_mode_effective: evidenceModeEffective ?? null,
            evidence_mode_decision_reason: evidenceModeReason ?? null,
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

  if (workspaceTabsEl) {
    workspaceTabsEl.addEventListener("keydown", handleWorkspaceTabKeydown);
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
