(() => {
  const startBtn = document.getElementById("start-btn");
  const newRunBtn = document.getElementById("new-run-btn");
  const fileInput = document.getElementById("vcf-file");
  const messageEl = document.getElementById("upload-validation-message");
  const resultsEl = document.getElementById("upload-validation-results");

  if (!startBtn || !fileInput || !messageEl || !resultsEl) {
    return;
  }

  const STORAGE_KEY = "sp_current_run";
  let inFlight = false;

  function clearEl(el) {
    while (el.firstChild) el.removeChild(el.firstChild);
  }

  function setMessage(kind, text) {
    clearEl(messageEl);
    if (!text) return;

    const alertEl = document.createElement("div");
    alertEl.className =
      kind === "error"
        ? "alert alert-danger py-2 mb-0"
        : kind === "success"
          ? "alert alert-success py-2 mb-0"
          : kind === "warning"
            ? "alert alert-warning py-2 mb-0"
            : "alert alert-info py-2 mb-0";
    alertEl.textContent = text;
    messageEl.appendChild(alertEl);
  }

  function addList(title, items) {
    if (!items || items.length === 0) return;

    const wrapper = document.createElement("div");
    wrapper.className = "mb-2";

    const heading = document.createElement("div");
    heading.className = "fw-semibold";
    heading.textContent = title;
    wrapper.appendChild(heading);

    const list = document.createElement("ul");
    list.className = "mb-0";
    for (const item of items) {
      const li = document.createElement("li");
      const msg = item?.message ?? String(item);
      li.textContent = msg;
      list.appendChild(li);
    }
    wrapper.appendChild(list);
    resultsEl.appendChild(wrapper);
  }

  function storeRun(run) {
    if (!run?.run_id) return;
    try {
      localStorage.setItem(
        STORAGE_KEY,
        JSON.stringify({
          run_id: run.run_id,
          status: run.status ?? null,
          created_at: run.created_at ?? null,
          reference_build: run.reference_build ?? null,
        }),
      );
    } catch {
      // ignore storage failures
    }
    dispatchRunChanged(run);
  }

  function dispatchRunChanged(run) {
    try {
      window.dispatchEvent(new CustomEvent("sp:run-changed", { detail: { run } }));
    } catch {
      // ignore event failures
    }
  }

  function dispatchTaskQueueReset() {
    try {
      window.dispatchEvent(new CustomEvent("sp:task-queue-reset"));
    } catch {
      // ignore event failures
    }
  }

  function clearStoredRun() {
    try {
      localStorage.removeItem(STORAGE_KEY);
    } catch {
      // ignore storage failures
    }
    dispatchRunChanged(null);
  }

  function loadStoredRun() {
    try {
      const stored = JSON.parse(localStorage.getItem(STORAGE_KEY));
      return stored && stored.run_id ? stored : null;
    } catch {
      return null;
    }
  }

  function setStartEnabled(enabled) {
    startBtn.disabled = !enabled || inFlight;
  }

  async function postMultipart(url, formData) {
    const resp = await fetch(url, {
      method: "POST",
      headers: { Accept: "application/json" },
      body: formData,
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

  window.addEventListener("sp:run-changed", (evt) => {
    const detail = evt?.detail ?? null;
    const run = detail?.run ?? detail ?? null;
    const status = run?.status ?? null;
    setStartEnabled(status !== "running");
  });

  setStartEnabled(loadStoredRun()?.status !== "running");

  function resetForNewInput() {
    const stored = loadStoredRun();
    if (stored?.status === "running") {
      setMessage(
        "warning",
        "A run is currently running. Cancel it before switching to a new file.",
      );
      return false;
    }

    dispatchTaskQueueReset();
    clearStoredRun();
    clearEl(resultsEl);
    return true;
  }

  startBtn.addEventListener("click", async () => {
    clearEl(resultsEl);
    setMessage(null, "");

    const file = fileInput.files?.[0];
    if (!file) {
      setMessage("error", "Choose a VCF file first.");
      return;
    }

    if (inFlight) return;
    inFlight = true;
    setStartEnabled(false);
    try {
      let runId = null;
      const stored = loadStoredRun();
      if (stored?.run_id && stored?.status === "running") {
        setMessage("error", "A run is already running. Cancel it before starting a new one.");
        return;
      }

      dispatchTaskQueueReset();
      setMessage("info", "Creating run...");
      const { resp: runResp, payload: runPayload } = await postJson("/api/v1/runs");
      if (!runResp.ok || !runPayload?.ok) {
        const msg = runPayload?.error?.message ?? "Failed to create run.";
        setMessage("error", msg);
        return;
      }

      runId = runPayload?.data?.run_id ?? null;
      if (!runId) {
        setMessage("error", "Failed to create run.");
        return;
      }
      storeRun(runPayload.data);

      const formData = new FormData();
      formData.append("vcf_file", file);

      setMessage("info", "Uploading and validating VCF...");
      let uploadResult = await postMultipart(
        `/api/v1/runs/${encodeURIComponent(runId)}/vcf`,
        formData,
      );

      if (uploadResult.resp.status === 404) {
        setMessage("error", "Run not found after creation. Refresh the page and try again.");
        return;
      }

      const { resp, payload } = uploadResult;

      if (!resp.ok || !payload?.ok) {
        const msg = payload?.error?.message ?? "Upload failed.";
        setMessage("error", msg);
        return;
      }

      const validation = payload?.data?.validation ?? null;
      const errors = validation?.errors ?? [];
      const warnings = validation?.warnings ?? [];

      if (errors.length > 0) {
        setMessage("error", "This file is invalid. Fix the errors and try again.");
        addList("Errors", errors);
        addList("Warnings", warnings);
        return;
      }

      if (warnings.length > 0) {
        setMessage("warning", "Validation passed with warnings. Starting the run...");
      } else {
        setMessage("info", "Validation passed. Starting the run...");
      }
      addList("Warnings", warnings);

      setMessage("info", "Starting pipeline...");
      const { resp: startResp, payload: startPayload } = await postJson(
        `/api/v1/runs/${encodeURIComponent(runId)}/start`,
      );
      if (!startResp.ok || !startPayload?.ok) {
        const msg = startPayload?.error?.message ?? "Failed to start run.";
        setMessage("error", msg);
        return;
      }

      storeRun(startPayload.data);
      try {
        const progressTab = document.getElementById("progress-tab");
        if (progressTab && window.bootstrap?.Tab) {
          window.bootstrap.Tab.getOrCreateInstance(progressTab).show();
        }
      } catch {
        // ignore tab failures
      }
      setMessage("success", "Run started.");
    } catch {
      setMessage("error", "Failed to start run.");
    } finally {
      inFlight = false;
      setStartEnabled(loadStoredRun()?.status !== "running");
    }
  });

  fileInput.addEventListener("change", () => {
    if (!fileInput.files || fileInput.files.length === 0) return;
    if (!resetForNewInput()) return;
    setMessage("info", "New file selected. Stage statuses were reset. Press Start to create a new run.");
  });

  if (newRunBtn) {
    newRunBtn.addEventListener("click", () => {
      if (!resetForNewInput()) return;
      setMessage("info", "Ready for a new run. Choose a file and press Start.");
    });
  }
})();
