(() => {
  const validateBtn = document.getElementById("upload-validate-btn");
  const fileInput = document.getElementById("vcf-file");
  const messageEl = document.getElementById("upload-validation-message");
  const resultsEl = document.getElementById("upload-validation-results");

  if (!validateBtn || !fileInput || !messageEl || !resultsEl) {
    return;
  }

  const STORAGE_KEY = "sp_current_run";

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

  function loadRunId() {
    try {
      const stored = JSON.parse(localStorage.getItem(STORAGE_KEY));
      return stored?.run_id ?? null;
    } catch {
      return null;
    }
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

  validateBtn.addEventListener("click", async () => {
    clearEl(resultsEl);
    setMessage(null, "");

    const runId = loadRunId();
    if (!runId) {
      setMessage("error", "Create a run first.");
      return;
    }

    const file = fileInput.files?.[0];
    if (!file) {
      setMessage("error", "Choose a VCF file to upload.");
      return;
    }

    validateBtn.disabled = true;
    try {
      const formData = new FormData();
      formData.append("vcf_file", file);

      const { resp, payload } = await postMultipart(
        `/api/v1/runs/${encodeURIComponent(runId)}/vcf`,
        formData,
      );

      if (!resp.ok || !payload?.ok) {
        const msg = payload?.error?.message ?? "Upload failed.";
        setMessage("error", msg);
        return;
      }

      const validation = payload?.data?.validation ?? null;
      const errors = validation?.errors ?? [];
      const warnings = validation?.warnings ?? [];

      if (errors.length > 0) {
        setMessage("error", "Validation failed. Fix the errors and re-upload.");
      } else if (warnings.length > 0) {
        setMessage("warning", "Validation passed with warnings.");
      } else {
        setMessage("success", "Validation passed.");
      }

      addList("Errors", errors);
      addList("Warnings", warnings);
    } catch {
      setMessage("error", "Upload failed.");
    } finally {
      validateBtn.disabled = false;
    }
  });

  async function loadExistingValidation() {
    const runId = loadRunId();
    if (!runId) return;

    try {
      const resp = await fetch(`/api/v1/runs/${encodeURIComponent(runId)}/vcf`, {
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

      if (!resp.ok || !payload?.ok) return;
      if (!payload.data) {
        setMessage("info", "No VCF uploaded for this run yet.");
        return;
      }

      const validation = payload?.data?.validation ?? null;
      const errors = validation?.errors ?? [];
      const warnings = validation?.warnings ?? [];

      if (errors.length > 0) {
        setMessage("error", "Latest validation failed. Fix the errors and re-upload.");
      } else if (warnings.length > 0) {
        setMessage("warning", "Latest validation passed with warnings.");
      } else {
        setMessage("success", "Latest validation passed.");
      }

      clearEl(resultsEl);
      addList("Errors", errors);
      addList("Warnings", warnings);
    } catch {
      // ignore on-load failures
    }
  }

  loadExistingValidation();
})();
