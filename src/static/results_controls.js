(() => {
  const finalMessageEl = document.getElementById("final-results-message");
  const parserMessageEl = document.getElementById("parser-results-message");
  const parserSummaryEl = document.getElementById("parser-results-summary");

  const preMessageEl = document.getElementById("pre-annotation-results-message");
  const preTableEl = document.getElementById("pre-annotation-results-table");
  const preBodyEl = document.getElementById("pre-annotation-results-body");

  const clsMessageEl = document.getElementById("classification-results-message");
  const clsTableEl = document.getElementById("classification-results-table");
  const clsBodyEl = document.getElementById("classification-results-body");

  const predMessageEl = document.getElementById("prediction-results-message");
  const predTableEl = document.getElementById("prediction-results-table");
  const predBodyEl = document.getElementById("prediction-results-body");

  const annotationMessageEl = document.getElementById("annotation-results-message");
  const annotationSummaryEl = document.getElementById("annotation-results-summary");
  const annotationVcfMessageEl = document.getElementById("annotation-vcf-message");
  const annotationVcfTableEl = document.getElementById("annotation-vcf-table");
  const annotationVcfHeadRowEl = document.getElementById("annotation-vcf-head-row");
  const annotationVcfBodyEl = document.getElementById("annotation-vcf-body");
  const reportingMessageEl = document.getElementById("reporting-results-message");
  const reportingSummaryEl = document.getElementById("reporting-results-summary");

  const offcanvasEl = document.getElementById("variant-details-offcanvas");
  const detailsMessageEl = document.getElementById("variant-details-message");
  const detailsKeyEl = document.getElementById("variant-details-key");
  const preBaseChangeEl = document.getElementById("variant-pre-base-change");
  const preSubstitutionClassEl = document.getElementById("variant-pre-substitution-class");
  const preRefClassEl = document.getElementById("variant-pre-ref-class");
  const preAltClassEl = document.getElementById("variant-pre-alt-class");
  const clsCategoryEl = document.getElementById("variant-cls-category");
  const clsReasonEl = document.getElementById("variant-cls-reason");
  const predictionsMessageEl = document.getElementById("variant-predictions-message");

  const predSiftOutcomeEl = document.getElementById("variant-pred-sift-outcome");
  const predSiftScoreEl = document.getElementById("variant-pred-sift-score");
  const predSiftLabelEl = document.getElementById("variant-pred-sift-label");
  const predSiftReasonEl = document.getElementById("variant-pred-sift-reason");
  const predSiftTimestampEl = document.getElementById("variant-pred-sift-timestamp");
  const predSiftSourceEl = document.getElementById("variant-pred-sift-source");

  const predPolyphen2OutcomeEl = document.getElementById("variant-pred-polyphen2-outcome");
  const predPolyphen2ScoreEl = document.getElementById("variant-pred-polyphen2-score");
  const predPolyphen2LabelEl = document.getElementById("variant-pred-polyphen2-label");
  const predPolyphen2ReasonEl = document.getElementById("variant-pred-polyphen2-reason");
  const predPolyphen2TimestampEl = document.getElementById("variant-pred-polyphen2-timestamp");
  const predPolyphen2SourceEl = document.getElementById("variant-pred-polyphen2-source");

  const predAlphamissenseOutcomeEl = document.getElementById("variant-pred-alphamissense-outcome");
  const predAlphamissenseScoreEl = document.getElementById("variant-pred-alphamissense-score");
  const predAlphamissenseLabelEl = document.getElementById("variant-pred-alphamissense-label");
  const predAlphamissenseReasonEl = document.getElementById("variant-pred-alphamissense-reason");
  const predAlphamissenseTimestampEl = document.getElementById("variant-pred-alphamissense-timestamp");
  const predAlphamissenseSourceEl = document.getElementById("variant-pred-alphamissense-source");
  const stageTabButtons = Array.from(
    document.querySelectorAll("#stage-results-tabs button[data-bs-toggle='tab']"),
  );

  if (
    !finalMessageEl ||
    !parserMessageEl ||
    !parserSummaryEl ||
    !preMessageEl ||
    !preTableEl ||
    !preBodyEl ||
    !clsMessageEl ||
    !clsTableEl ||
    !clsBodyEl ||
    !predMessageEl ||
    !predTableEl ||
    !predBodyEl ||
    !annotationMessageEl ||
    !annotationSummaryEl ||
    !annotationVcfMessageEl ||
    !annotationVcfTableEl ||
    !annotationVcfHeadRowEl ||
    !annotationVcfBodyEl ||
    !reportingMessageEl ||
    !reportingSummaryEl
  ) {
    return;
  }

  const STORAGE_KEY = "sp_current_run";
  const RESULTS_STAGE_TAB_KEY_PREFIX = "sp_results_active_stage_tab";
  const DEFAULT_STAGE_TAB_ID = "stage-results-final-tab";
  const stageTabIdSet = new Set(stageTabButtons.map((btn) => btn?.id).filter((value) => Boolean(value)));
  let inFlight = false;
  let refreshTimerId = null;
  let pausePolling = false;
  let lastTriggerEl = null;

  function clearEl(el) {
    while (el.firstChild) el.removeChild(el.firstChild);
  }

  function setText(el, text) {
    if (!el) return;
    el.textContent = text || "\u2014";
  }

  function setMessage(el, text) {
    if (!el) return;
    clearEl(el);
    if (!text) return;
    const span = document.createElement("span");
    span.textContent = text;
    el.appendChild(span);
  }

  function hideTable(tableEl, bodyEl) {
    tableEl.hidden = true;
    clearEl(bodyEl);
  }

  function hideAnnotationVcfTable() {
    annotationVcfTableEl.hidden = true;
    clearEl(annotationVcfHeadRowEl);
    clearEl(annotationVcfBodyEl);
  }

  function parseVcfPreviewLines(lines) {
    let headerCols = null;
    const dataRows = [];

    for (const line of lines) {
      if (typeof line !== "string") continue;
      if (line.startsWith("#CHROM\t")) {
        headerCols = line.replace(/^#/, "").split("\t");
        continue;
      }
      if (line.startsWith("#")) continue;
      if (!line.trim()) continue;
      dataRows.push(line.split("\t"));
    }

    if (!headerCols || headerCols.length === 0) return null;
    return { headerCols, dataRows };
  }

  function renderAnnotationVcfTable(parsed) {
    hideAnnotationVcfTable();
    if (!parsed) return;

    for (const col of parsed.headerCols) {
      const th = document.createElement("th");
      th.scope = "col";
      th.textContent = col || "\u2014";
      annotationVcfHeadRowEl.appendChild(th);
    }

    const expectedCols = parsed.headerCols.length;
    for (const rawRow of parsed.dataRows) {
      const tr = document.createElement("tr");
      const row = Array.isArray(rawRow) ? rawRow.slice() : [];
      if (row.length > expectedCols && expectedCols > 0) {
        const collapsed = row.slice(expectedCols - 1).join("\t");
        row.length = expectedCols - 1;
        row.push(collapsed);
      }

      for (let idx = 0; idx < expectedCols; idx += 1) {
        const td = document.createElement("td");
        td.textContent = idx < row.length ? String(row[idx] ?? "") : "";
        tr.appendChild(td);
      }
      annotationVcfBodyEl.appendChild(tr);
    }

    annotationVcfTableEl.hidden = false;
  }

  function basename(path) {
    if (!path) return "";
    const asText = String(path);
    const parts = asText.split(/[/\\\\]+/);
    return parts[parts.length - 1] || asText;
  }

  function setSummaryRows(containerEl, rows) {
    clearEl(containerEl);
    if (!rows || rows.length === 0) return;
    const list = document.createElement("ul");
    list.className = "list-unstyled mb-0";

    for (const row of rows) {
      if (row?.value == null || row.value === "") continue;
      const li = document.createElement("li");
      li.className = "mb-1";

      const label = document.createElement("span");
      label.className = "text-secondary";
      label.textContent = `${row.label}: `;
      li.appendChild(label);

      const valueCode = document.createElement("code");
      valueCode.textContent = String(row.value);
      li.appendChild(valueCode);
      list.appendChild(li);
    }

    containerEl.appendChild(list);
  }

  function loadRunId() {
    try {
      const stored = JSON.parse(localStorage.getItem(STORAGE_KEY));
      return stored?.run_id ?? null;
    } catch {
      return null;
    }
  }

  function stageTabStorageKey(runId) {
    return `${RESULTS_STAGE_TAB_KEY_PREFIX}:${runId}`;
  }

  function activeStageTabId() {
    const activeBtn = document.querySelector("#stage-results-tabs button.nav-link.active");
    return activeBtn?.id ?? null;
  }

  function showStageTab(tabId) {
    if (!tabId || !stageTabIdSet.has(tabId)) return;
    if (activeStageTabId() === tabId) return;

    const button = document.getElementById(tabId);
    if (!button) return;
    if (!window.bootstrap?.Tab) return;

    try {
      window.bootstrap.Tab.getOrCreateInstance(button).show();
    } catch {
      // ignore tab render errors
    }
  }

  function restoreStageTabForCurrentRun() {
    const runId = loadRunId();
    if (!runId) {
      showStageTab(DEFAULT_STAGE_TAB_ID);
      return;
    }

    let storedTabId = null;
    try {
      storedTabId = localStorage.getItem(stageTabStorageKey(runId));
    } catch {
      storedTabId = null;
    }
    showStageTab(stageTabIdSet.has(storedTabId) ? storedTabId : DEFAULT_STAGE_TAB_ID);
  }

  function persistStageTabForCurrentRun(tabId) {
    if (!tabId || !stageTabIdSet.has(tabId)) return;
    const runId = loadRunId();
    if (!runId) return;
    try {
      localStorage.setItem(stageTabStorageKey(runId), tabId);
    } catch {
      // ignore storage failures
    }
  }

  function stageStatusText(stageStatus, pendingText) {
    if (stageStatus === "running") return "Stage is running...";
    if (stageStatus === "queued") return pendingText;
    if (stageStatus === "canceled") return "Stage was canceled.";
    return "";
  }

  function stageFailureText(stage, fallback) {
    if (!stage || stage.status !== "failed") return null;
    const errorMessage = stage?.error?.message;
    const errorCode = stage?.error?.code;
    if (errorCode && errorMessage) return `${errorCode}: ${errorMessage}`;
    if (errorMessage) return errorMessage;
    return fallback;
  }

  function stageByName(stages, stageName) {
    if (!Array.isArray(stages)) return null;
    for (const stage of stages) {
      if ((stage?.stage_name ?? null) === stageName) return stage;
    }
    return null;
  }

  function chooseNextInterval(stages) {
    if (!Array.isArray(stages) || stages.length === 0) return 1500;
    const hasRunning = stages.some((stage) => stage?.status === "running");
    if (hasRunning) return 900;
    const hasQueued = stages.some((stage) => stage?.status === "queued");
    if (hasQueued) return 1600;
    return 5000;
  }

  function scheduleNextRefresh(ms) {
    if (refreshTimerId != null) {
      window.clearTimeout(refreshTimerId);
      refreshTimerId = null;
    }
    if (pausePolling) return;
    refreshTimerId = window.setTimeout(() => void refresh(), ms);
  }

  function pauseRefresh() {
    pausePolling = true;
    if (refreshTimerId != null) {
      window.clearTimeout(refreshTimerId);
      refreshTimerId = null;
    }
  }

  function resumeAfterDrawerFailure() {
    pausePolling = false;
    clearDetails();
    scheduleNextRefresh(0);
  }

  function resetTaskQueueState() {
    pausePolling = false;
    if (refreshTimerId != null) {
      window.clearTimeout(refreshTimerId);
      refreshTimerId = null;
    }
    inFlight = false;
    clearDetails();
    setMessage(finalMessageEl, "Choose a VCF file and press Start.");
    setMessage(parserMessageEl, "Parser results will appear after run start.");
    setSummaryRows(parserSummaryEl, []);
    setMessage(preMessageEl, "Pre-annotation results will appear after stage completion.");
    hideTable(preTableEl, preBodyEl);
    setMessage(clsMessageEl, "Classification results will appear after stage completion.");
    hideTable(clsTableEl, clsBodyEl);
    setMessage(predMessageEl, "Prediction results will appear after stage completion.");
    hideTable(predTableEl, predBodyEl);
    setMessage(annotationMessageEl, "Annotation summary will appear after stage completion.");
    setSummaryRows(annotationSummaryEl, []);
    setMessage(annotationVcfMessageEl, "Annotated VCF preview will appear after stage completion.");
    hideAnnotationVcfTable();
    setMessage(reportingMessageEl, "Reporting summary will appear after stage completion.");
    setSummaryRows(reportingSummaryEl, []);
  }

  async function getJson(url) {
    const resp = await fetch(url, { method: "GET", headers: { Accept: "application/json" } });
    const text = await resp.text();
    let payload = null;
    try {
      payload = JSON.parse(text);
    } catch {
      payload = null;
    }
    return { resp, payload };
  }

  function predictorLabel(predictorKey) {
    if (predictorKey === "sift") return "SIFT";
    if (predictorKey === "polyphen2") return "PolyPhen-2";
    if (predictorKey === "alphamissense") return "AlphaMissense";
    return predictorKey || "\u2014";
  }

  function renderParser(stage) {
    const failed = stageFailureText(stage, "Parser failed.");
    if (failed) {
      setMessage(parserMessageEl, failed);
      setSummaryRows(parserSummaryEl, []);
      return;
    }

    if (!stage || stage.status !== "succeeded") {
      setMessage(parserMessageEl, stageStatusText(stage?.status, "Waiting for parser."));
      setSummaryRows(parserSummaryEl, []);
      return;
    }

    const stats = stage?.stats ?? {};
    setMessage(parserMessageEl, "Parser output is ready.");
    setSummaryRows(parserSummaryEl, [
      { label: "SNVs persisted", value: stats?.snv_records_persisted },
      { label: "Duplicates ignored", value: stats?.duplicate_records_ignored },
      { label: "Header lines", value: stats?.header_lines },
      { label: "Records scanned", value: stats?.total_records_scanned },
    ]);
  }

  function renderPreRows(rows) {
    clearEl(preBodyEl);
    for (const row of rows) {
      const tr = document.createElement("tr");

      const variantCell = document.createElement("td");
      variantCell.className = "text-nowrap";
      variantCell.textContent = row?.variant_key ?? "\u2014";
      tr.appendChild(variantCell);

      const baseChangeCell = document.createElement("td");
      baseChangeCell.className = "text-nowrap";
      baseChangeCell.textContent = row?.base_change ?? "\u2014";
      tr.appendChild(baseChangeCell);

      const substitutionCell = document.createElement("td");
      substitutionCell.className = "text-nowrap";
      substitutionCell.textContent = row?.substitution_class ?? "\u2014";
      tr.appendChild(substitutionCell);

      const refClassCell = document.createElement("td");
      refClassCell.className = "text-nowrap";
      refClassCell.textContent = row?.ref_class ?? "\u2014";
      tr.appendChild(refClassCell);

      const altClassCell = document.createElement("td");
      altClassCell.className = "text-nowrap";
      altClassCell.textContent = row?.alt_class ?? "\u2014";
      tr.appendChild(altClassCell);

      preBodyEl.appendChild(tr);
    }
    preTableEl.hidden = false;
  }

  function renderClassificationRows(rows) {
    clearEl(clsBodyEl);
    for (const row of rows) {
      const tr = document.createElement("tr");

      const variantCell = document.createElement("td");
      variantCell.className = "text-nowrap";
      const variantKey = row?.variant_key ?? "\u2014";
      if (offcanvasEl && row?.variant_id) {
        const btn = document.createElement("button");
        btn.type = "button";
        btn.className = "btn btn-link p-0 align-baseline";
        btn.setAttribute("aria-controls", "variant-details-offcanvas");
        btn.setAttribute("aria-expanded", "false");
        btn.textContent = variantKey;
        btn.addEventListener("click", () => void openVariantDetails(row, btn));
        variantCell.appendChild(btn);
      } else {
        variantCell.textContent = variantKey;
      }
      tr.appendChild(variantCell);

      const consequenceCell = document.createElement("td");
      consequenceCell.className = "text-nowrap";
      consequenceCell.textContent = row?.consequence_category ?? "\u2014";
      tr.appendChild(consequenceCell);

      const reasonCell = document.createElement("td");
      const reason =
        row?.consequence_category === "unclassified"
          ? row?.reason_message || row?.reason_code || "Unclassified."
          : row?.reason_message || "\u2014";
      reasonCell.textContent = reason;
      tr.appendChild(reasonCell);

      clsBodyEl.appendChild(tr);
    }
    clsTableEl.hidden = false;
  }

  function renderPredictionRows(rows) {
    clearEl(predBodyEl);
    for (const row of rows) {
      const tr = document.createElement("tr");

      const variantCell = document.createElement("td");
      variantCell.className = "text-nowrap";
      variantCell.textContent = row?.variant_key ?? "\u2014";
      tr.appendChild(variantCell);

      const predictorCell = document.createElement("td");
      predictorCell.className = "text-nowrap";
      predictorCell.textContent = predictorLabel(row?.predictor_key);
      tr.appendChild(predictorCell);

      const outcomeCell = document.createElement("td");
      outcomeCell.className = "text-nowrap";
      outcomeCell.textContent = row?.outcome ?? "\u2014";
      tr.appendChild(outcomeCell);

      const scoreCell = document.createElement("td");
      scoreCell.className = "text-nowrap";
      scoreCell.textContent = row?.score != null ? String(row.score) : "\u2014";
      tr.appendChild(scoreCell);

      const labelCell = document.createElement("td");
      labelCell.className = "text-nowrap";
      labelCell.textContent = row?.label != null && row?.label !== "" ? String(row.label) : "\u2014";
      tr.appendChild(labelCell);

      const reasonCell = document.createElement("td");
      reasonCell.textContent = row?.reason_message || row?.reason_code || "\u2014";
      tr.appendChild(reasonCell);

      predBodyEl.appendChild(tr);
    }
    predTableEl.hidden = false;
  }

  function renderAnnotation(stage) {
    const failed = stageFailureText(stage, "Annotation failed.");
    if (failed) {
      setMessage(annotationMessageEl, failed);
      setSummaryRows(annotationSummaryEl, []);
      setMessage(annotationVcfMessageEl, "");
      hideAnnotationVcfTable();
      return;
    }

    if (!stage || stage.status !== "succeeded") {
      setMessage(annotationMessageEl, stageStatusText(stage?.status, "Waiting for annotation."));
      setSummaryRows(annotationSummaryEl, []);
      setMessage(annotationVcfMessageEl, stageStatusText(stage?.status, "Waiting for annotated VCF preview."));
      hideAnnotationVcfTable();
      return;
    }

    const stats = stage?.stats ?? {};
    setMessage(annotationMessageEl, "Annotation output is ready.");
    setSummaryRows(annotationSummaryEl, [
      { label: "Tool", value: stats?.tool },
      { label: "Genome", value: stats?.genome },
      { label: "Configured", value: stats?.configured },
      { label: "Variants written", value: stats?.variants_written },
      { label: "Output VCF", value: basename(stats?.output_vcf_path) || stats?.output_vcf_path },
      { label: "Exit code", value: stats?.snpeff_exit_code },
      { label: "Note", value: stats?.note },
    ]);
  }

  async function refreshAnnotationOutput(runId, stage) {
    const failed = stageFailureText(stage, "Annotation failed.");
    if (failed) {
      setMessage(annotationVcfMessageEl, "");
      hideAnnotationVcfTable();
      return;
    }
    if (!stage || stage.status !== "succeeded") {
      setMessage(annotationVcfMessageEl, stageStatusText(stage?.status, "Waiting for annotated VCF preview."));
      hideAnnotationVcfTable();
      return;
    }

    try {
      const { resp, payload } = await getJson(
        `/api/v1/runs/${encodeURIComponent(runId)}/annotation_output?limit=400`,
      );
      if (!resp.ok || !payload?.ok) {
        setMessage(
          annotationVcfMessageEl,
          payload?.error?.message || "Unable to load annotated VCF preview.",
        );
        hideAnnotationVcfTable();
        return;
      }

      const lines = Array.isArray(payload?.data?.preview_lines) ? payload.data.preview_lines : [];
      const truncated = Boolean(payload?.data?.truncated);
      if (lines.length === 0) {
        setMessage(
          annotationVcfMessageEl,
          "Annotation stage succeeded but annotated VCF content is not available for preview.",
        );
        hideAnnotationVcfTable();
        return;
      }

      const parsed = parseVcfPreviewLines(lines);
      if (!parsed) {
        setMessage(annotationVcfMessageEl, "Annotated VCF preview is available but could not be parsed as tabular VCF.");
        hideAnnotationVcfTable();
        return;
      }

      setMessage(
        annotationVcfMessageEl,
        truncated
          ? `Showing first ${parsed.dataRows.length} variant rows from ${lines.length} preview lines.`
          : `Showing ${parsed.dataRows.length} variant rows.`,
      );
      renderAnnotationVcfTable(parsed);
    } catch {
      setMessage(annotationVcfMessageEl, "Unable to load annotated VCF preview.");
      hideAnnotationVcfTable();
    }
  }

  function renderReporting(stage) {
    const failed = stageFailureText(stage, "Reporting failed.");
    if (failed) {
      setMessage(reportingMessageEl, failed);
      setSummaryRows(reportingSummaryEl, []);
      return;
    }

    if (!stage || stage.status !== "succeeded") {
      setMessage(reportingMessageEl, stageStatusText(stage?.status, "Waiting for reporting."));
      setSummaryRows(reportingSummaryEl, []);
      return;
    }

    const stats = stage?.stats ?? {};
    setMessage(reportingMessageEl, "Reporting output is ready.");
    const rows = [];
    const keys = Object.keys(stats);
    if (keys.length === 0) {
      rows.push({ label: "Status", value: "Reporting stage succeeded." });
    } else {
      for (const key of keys) {
        rows.push({ label: key, value: stats[key] });
      }
    }
    setSummaryRows(reportingSummaryEl, rows);
  }

  function renderFinalResult(stages) {
    if (!Array.isArray(stages) || stages.length === 0) {
      setMessage(finalMessageEl, "Start a run to view partial and final results.");
      return;
    }

    const reportingStage = stageByName(stages, "reporting");
    const failedStage = stages.find((stage) => stage?.status === "failed");
    const runningStage = stages.find((stage) => stage?.status === "running");
    const succeededStages = stages.filter((stage) => stage?.status === "succeeded");

    if (reportingStage?.status === "succeeded") {
      setMessage(finalMessageEl, "Final result is ready. Reporting stage succeeded.");
      return;
    }
    if (reportingStage?.status === "failed") {
      setMessage(
        finalMessageEl,
        reportingStage?.error?.message || "Final result is not available because reporting failed.",
      );
      return;
    }
    if (reportingStage?.status === "canceled") {
      setMessage(finalMessageEl, "Final result is not available because the run was canceled.");
      return;
    }
    if (failedStage) {
      const stageName = String(failedStage.stage_name || "pipeline");
      const stageMessage = failedStage?.error?.message || "Stage failed.";
      setMessage(finalMessageEl, `Run failed at ${stageName}: ${stageMessage}`);
      return;
    }
    if (runningStage) {
      const stageName = String(runningStage.stage_name || "pipeline");
      setMessage(finalMessageEl, `Pipeline is running (${stageName}). Partial results are shown below.`);
      return;
    }
    if (succeededStages.length > 0) {
      setMessage(finalMessageEl, "Partial results are available. Final result will appear after reporting succeeds.");
      return;
    }

    setMessage(finalMessageEl, "Pipeline is queued. Partial results will appear as stages complete.");
  }

  async function refreshPreAnnotations(runId, stage) {
    const failed = stageFailureText(stage, "Pre-annotation failed.");
    if (failed) {
      hideTable(preTableEl, preBodyEl);
      setMessage(preMessageEl, failed);
      return;
    }
    if (!stage || stage.status !== "succeeded") {
      hideTable(preTableEl, preBodyEl);
      setMessage(preMessageEl, stageStatusText(stage?.status, "Waiting for pre-annotation."));
      return;
    }

    try {
      const { resp, payload } = await getJson(
        `/api/v1/runs/${encodeURIComponent(runId)}/pre_annotations?limit=200`,
      );
      if (!resp.ok || !payload?.ok) {
        hideTable(preTableEl, preBodyEl);
        setMessage(preMessageEl, payload?.error?.message || "Unable to load pre-annotation output.");
        return;
      }
      const rows = Array.isArray(payload?.data?.pre_annotations) ? payload.data.pre_annotations : [];
      if (rows.length === 0) {
        hideTable(preTableEl, preBodyEl);
        setMessage(preMessageEl, "Pre-annotation stage succeeded but no rows are available for current upload.");
        return;
      }
      renderPreRows(rows);
      setMessage(preMessageEl, "");
    } catch {
      hideTable(preTableEl, preBodyEl);
      setMessage(preMessageEl, "Unable to load pre-annotation output.");
    }
  }

  async function refreshClassifications(runId, stage) {
    const failed = stageFailureText(stage, "Classification failed.");
    if (failed) {
      hideTable(clsTableEl, clsBodyEl);
      setMessage(clsMessageEl, failed);
      return;
    }
    if (!stage || stage.status !== "succeeded") {
      hideTable(clsTableEl, clsBodyEl);
      setMessage(clsMessageEl, stageStatusText(stage?.status, "Waiting for classification."));
      return;
    }

    try {
      const { resp, payload } = await getJson(
        `/api/v1/runs/${encodeURIComponent(runId)}/classifications?limit=200`,
      );
      if (!resp.ok || !payload?.ok) {
        hideTable(clsTableEl, clsBodyEl);
        setMessage(clsMessageEl, payload?.error?.message || "Unable to load classification output.");
        return;
      }
      const rows = Array.isArray(payload?.data?.classifications) ? payload.data.classifications : [];
      if (rows.length === 0) {
        hideTable(clsTableEl, clsBodyEl);
        setMessage(clsMessageEl, "Classification stage succeeded but no rows are available for current upload.");
        return;
      }
      renderClassificationRows(rows);
      setMessage(clsMessageEl, "");
    } catch {
      hideTable(clsTableEl, clsBodyEl);
      setMessage(clsMessageEl, "Unable to load classification output.");
    }
  }

  async function refreshPredictions(runId, stage) {
    const failed = stageFailureText(stage, "Prediction failed.");
    if (failed) {
      hideTable(predTableEl, predBodyEl);
      setMessage(predMessageEl, failed);
      return;
    }
    if (!stage || stage.status !== "succeeded") {
      hideTable(predTableEl, predBodyEl);
      setMessage(predMessageEl, stageStatusText(stage?.status, "Waiting for prediction."));
      return;
    }

    try {
      const { resp, payload } = await getJson(
        `/api/v1/runs/${encodeURIComponent(runId)}/predictor_outputs?limit=500`,
      );
      if (!resp.ok || !payload?.ok) {
        hideTable(predTableEl, predBodyEl);
        setMessage(predMessageEl, payload?.error?.message || "Unable to load prediction output.");
        return;
      }
      const rows = Array.isArray(payload?.data?.predictor_outputs) ? payload.data.predictor_outputs : [];
      if (rows.length === 0) {
        hideTable(predTableEl, predBodyEl);
        setMessage(predMessageEl, "Prediction stage succeeded but no rows are available for current upload.");
        return;
      }
      renderPredictionRows(rows);
      setMessage(predMessageEl, "");
    } catch {
      hideTable(predTableEl, predBodyEl);
      setMessage(predMessageEl, "Unable to load prediction output.");
    }
  }

  function setDetailsMessage(text) {
    setMessage(detailsMessageEl, text);
  }

  function setPredictionsMessage(text) {
    setMessage(predictionsMessageEl, text);
  }

  function clearDetails() {
    setDetailsMessage("");
    setText(detailsKeyEl, "\u2014");
    setText(preBaseChangeEl, "\u2014");
    setText(preSubstitutionClassEl, "\u2014");
    setText(preRefClassEl, "\u2014");
    setText(preAltClassEl, "\u2014");
    setText(clsCategoryEl, "\u2014");
    setText(clsReasonEl, "\u2014");

    setPredictionsMessage("");

    setText(predSiftOutcomeEl, "\u2014");
    setText(predSiftScoreEl, "\u2014");
    setText(predSiftLabelEl, "\u2014");
    setText(predSiftReasonEl, "\u2014");
    setText(predSiftTimestampEl, "\u2014");
    setText(predSiftSourceEl, "\u2014");

    setText(predPolyphen2OutcomeEl, "\u2014");
    setText(predPolyphen2ScoreEl, "\u2014");
    setText(predPolyphen2LabelEl, "\u2014");
    setText(predPolyphen2ReasonEl, "\u2014");
    setText(predPolyphen2TimestampEl, "\u2014");
    setText(predPolyphen2SourceEl, "\u2014");

    setText(predAlphamissenseOutcomeEl, "\u2014");
    setText(predAlphamissenseScoreEl, "\u2014");
    setText(predAlphamissenseLabelEl, "\u2014");
    setText(predAlphamissenseReasonEl, "\u2014");
    setText(predAlphamissenseTimestampEl, "\u2014");
    setText(predAlphamissenseSourceEl, "\u2014");
  }

  if (offcanvasEl) {
    offcanvasEl.addEventListener("shown.bs.offcanvas", () => {
      pauseRefresh();
    });
    offcanvasEl.addEventListener("hidden.bs.offcanvas", () => {
      pausePolling = false;
      const toFocus = lastTriggerEl;
      lastTriggerEl = null;
      if (toFocus && typeof toFocus.setAttribute === "function") {
        try {
          toFocus.setAttribute("aria-expanded", "false");
        } catch {
          // keep cleanup/resume path running even if focus management fails
        }
      }
      if (toFocus && typeof toFocus.focus === "function") {
        try {
          toFocus.focus();
        } catch {
          // keep cleanup/resume path running even if focus management fails
        }
      }
      clearDetails();
      void refresh();
    });
  }

  for (const tabButton of stageTabButtons) {
    tabButton.addEventListener("shown.bs.tab", (event) => {
      persistStageTabForCurrentRun(event?.target?.id ?? null);
    });
  }

  window.addEventListener("sp:run-changed", () => {
    pausePolling = false;
    restoreStageTabForCurrentRun();
    void refresh();
  });

  window.addEventListener("sp:task-queue-reset", () => {
    resetTaskQueueState();
  });

  async function openVariantDetails(row, triggerEl) {
    if (!offcanvasEl) return;
    if (!window.bootstrap?.Offcanvas) return;

    const runId = loadRunId();
    if (!runId) return;

    lastTriggerEl = triggerEl || null;
    if (lastTriggerEl && typeof lastTriggerEl.setAttribute === "function") {
      try {
        lastTriggerEl.setAttribute("aria-expanded", "true");
      } catch {
        // best-effort accessibility hint only
      }
    }

    pauseRefresh();
    clearDetails();
    setDetailsMessage("Loading variant details...");
    setPredictionsMessage("Loading predictor outputs...");
    setText(detailsKeyEl, row?.variant_key ?? "\u2014");
    setText(clsCategoryEl, row?.consequence_category ?? "\u2014");
    const reason =
      row?.consequence_category === "unclassified"
        ? row?.reason_message || row?.reason_code || "Unclassified."
        : row?.reason_message || "\u2014";
    setText(clsReasonEl, reason);

    try {
      window.bootstrap.Offcanvas.getOrCreateInstance(offcanvasEl).show();
    } catch {
      resumeAfterDrawerFailure();
      return;
    }

    try {
      const { resp, payload } = await getJson(
        `/api/v1/runs/${encodeURIComponent(runId)}/pre_annotations?variant_id=${encodeURIComponent(row?.variant_id || "")}`,
      );
      if (resp.status === 404) {
        setDetailsMessage("No run found.");
      } else if (!resp.ok || !payload?.ok) {
        setDetailsMessage(payload?.error?.message || "Unable to load variant details.");
      } else {
        const stage = payload?.data?.stage ?? null;
        const stageStatus = stage?.status ?? null;
        const rows = payload?.data?.pre_annotations ?? [];
        if (!rows || rows.length === 0) {
          if (stageStatus === "running") {
            setDetailsMessage("Pre-annotation running...");
          } else if (stageStatus === "failed") {
            setDetailsMessage(stage?.error?.message || "Pre-annotation failed.");
          } else if (stageStatus === "canceled") {
            setDetailsMessage("Pre-annotation was canceled.");
          } else {
            setDetailsMessage("Not available for current upload yet.");
          }
        } else {
          const pre = rows[0];
          setDetailsMessage("");
          setText(preBaseChangeEl, pre?.base_change ?? "\u2014");
          setText(preSubstitutionClassEl, pre?.substitution_class ?? "\u2014");
          setText(preRefClassEl, pre?.ref_class ?? "\u2014");
          setText(preAltClassEl, pre?.alt_class ?? "\u2014");
        }
      }
    } catch {
      setDetailsMessage("Unable to load variant details.");
    }

    try {
      const { resp, payload } = await getJson(
        `/api/v1/runs/${encodeURIComponent(runId)}/predictor_outputs?variant_id=${encodeURIComponent(row?.variant_id || "")}`,
      );
      if (resp.status === 404) {
        setPredictionsMessage("No run found.");
      } else if (!resp.ok || !payload?.ok) {
        setPredictionsMessage(payload?.error?.message || "Unable to load predictor outputs.");
      } else {
        const stage = payload?.data?.stage ?? null;
        const stageStatus = stage?.status ?? null;
        const rows = payload?.data?.predictor_outputs ?? [];

        if (!rows || rows.length === 0) {
          if (stageStatus === "running") {
            setPredictionsMessage("Prediction running...");
          } else if (stageStatus === "failed") {
            setPredictionsMessage(stage?.error?.message || "Prediction failed.");
          } else if (stageStatus === "canceled") {
            setPredictionsMessage("Prediction was canceled.");
          } else {
            setPredictionsMessage("Not available for current upload yet.");
          }
        } else {
          const byKey = new Map();
          for (const output of rows) {
            const key = output?.predictor_key ?? null;
            if (key) byKey.set(key, output);
          }

          function renderPredictor(predictorKey, target) {
            const output = byKey.get(predictorKey) || null;
            if (!output) {
              setText(target.outcomeEl, "not available");
              setText(target.scoreEl, "\u2014");
              setText(target.labelEl, "\u2014");
              setText(target.reasonEl, "\u2014");
              setText(target.timestampEl, "\u2014");
              setText(target.sourceEl, predictorKey);
              return;
            }
            setText(target.outcomeEl, output?.outcome ?? "\u2014");
            setText(target.scoreEl, output?.score != null ? String(output.score) : "\u2014");
            setText(target.labelEl, output?.label != null && output?.label !== "" ? String(output.label) : "\u2014");
            setText(target.reasonEl, output?.reason_message || output?.reason_code || "\u2014");
            setText(target.timestampEl, output?.created_at ? String(output.created_at) : "\u2014");
            setText(target.sourceEl, output?.predictor_key ? String(output.predictor_key) : predictorKey);
          }

          renderPredictor("sift", {
            outcomeEl: predSiftOutcomeEl,
            scoreEl: predSiftScoreEl,
            labelEl: predSiftLabelEl,
            reasonEl: predSiftReasonEl,
            timestampEl: predSiftTimestampEl,
            sourceEl: predSiftSourceEl,
          });
          renderPredictor("polyphen2", {
            outcomeEl: predPolyphen2OutcomeEl,
            scoreEl: predPolyphen2ScoreEl,
            labelEl: predPolyphen2LabelEl,
            reasonEl: predPolyphen2ReasonEl,
            timestampEl: predPolyphen2TimestampEl,
            sourceEl: predPolyphen2SourceEl,
          });
          renderPredictor("alphamissense", {
            outcomeEl: predAlphamissenseOutcomeEl,
            scoreEl: predAlphamissenseScoreEl,
            labelEl: predAlphamissenseLabelEl,
            reasonEl: predAlphamissenseReasonEl,
            timestampEl: predAlphamissenseTimestampEl,
            sourceEl: predAlphamissenseSourceEl,
          });
          setPredictionsMessage("");
        }
      }
    } catch {
      setPredictionsMessage("Unable to load predictor outputs.");
    }
  }

  function resetToNoRun() {
    setMessage(finalMessageEl, "Start a run to view partial and final results.");
    setMessage(parserMessageEl, "Start a run to see parser output.");
    setSummaryRows(parserSummaryEl, []);

    hideTable(preTableEl, preBodyEl);
    setMessage(preMessageEl, "Start a run to see pre-annotation output.");

    hideTable(clsTableEl, clsBodyEl);
    setMessage(clsMessageEl, "Start a run to see classification output.");

    hideTable(predTableEl, predBodyEl);
    setMessage(predMessageEl, "Start a run to see prediction output.");

    setMessage(annotationMessageEl, "Start a run to see annotation output.");
    setSummaryRows(annotationSummaryEl, []);
    setMessage(annotationVcfMessageEl, "Start a run to see annotated VCF preview.");
    hideAnnotationVcfTable();

    setMessage(reportingMessageEl, "Start a run to see reporting output.");
    setSummaryRows(reportingSummaryEl, []);
  }

  async function refresh() {
    if (pausePolling) return;
    const runId = loadRunId();
    if (!runId) {
      resetToNoRun();
      scheduleNextRefresh(1800);
      return;
    }
    if (inFlight) return;
    inFlight = true;

    try {
      const { resp, payload } = await getJson(`/api/v1/runs/${encodeURIComponent(runId)}/stages`);
      if (resp.status === 404) {
        resetToNoRun();
        scheduleNextRefresh(2500);
        return;
      }
      if (!resp.ok || !payload?.ok) {
        const message = payload?.error?.message || "Unable to load stage outputs.";
        setMessage(finalMessageEl, message);
        scheduleNextRefresh(2500);
        return;
      }

      const stages = Array.isArray(payload?.data?.stages) ? payload.data.stages : [];
      renderFinalResult(stages);

      const parserStage = stageByName(stages, "parser");
      const preStage = stageByName(stages, "pre_annotation");
      const classificationStage = stageByName(stages, "classification");
      const predictionStage = stageByName(stages, "prediction");
      const annotationStage = stageByName(stages, "annotation");
      const reportingStage = stageByName(stages, "reporting");

      renderParser(parserStage);
      await refreshPreAnnotations(runId, preStage);
      await refreshClassifications(runId, classificationStage);
      await refreshPredictions(runId, predictionStage);
      renderAnnotation(annotationStage);
      await refreshAnnotationOutput(runId, annotationStage);
      renderReporting(reportingStage);

      scheduleNextRefresh(chooseNextInterval(stages));
    } catch {
      setMessage(finalMessageEl, "Unable to load results right now.");
      scheduleNextRefresh(2500);
    } finally {
      inFlight = false;
    }
  }

  restoreStageTabForCurrentRun();
  void refresh();
})();
