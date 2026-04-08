(() => {
  const finalMessageEl = document.getElementById("final-results-message");
  const parserMessageEl = document.getElementById("parser-results-message");
  const parserSummaryEl = document.getElementById("parser-results-summary");

  const preMessageEl = document.getElementById("pre-annotation-results-message");
  const preTableEl = document.getElementById("pre-annotation-results-table");
  const preBodyEl = document.getElementById("pre-annotation-results-body");
  const prePagePrevEl = document.getElementById("pre-annotation-page-prev");
  const prePageNextEl = document.getElementById("pre-annotation-page-next");
  const prePageLabelEl = document.getElementById("pre-annotation-page-label");

  const clsMessageEl = document.getElementById("classification-results-message");
  const clsTableEl = document.getElementById("classification-results-table");
  const clsBodyEl = document.getElementById("classification-results-body");
  const clsFilterEl = document.getElementById("classification-filter");
  const clsPagePrevEl = document.getElementById("classification-page-prev");
  const clsPageNextEl = document.getElementById("classification-page-next");
  const clsPageLabelEl = document.getElementById("classification-page-label");

  const predMessageEl = document.getElementById("prediction-results-message");
  const predTableEl = document.getElementById("prediction-results-table");
  const predBodyEl = document.getElementById("prediction-results-body");
  const predShowNotApplicableEl = document.getElementById("prediction-show-not-applicable");
  const predPagePrevEl = document.getElementById("prediction-page-prev");
  const predPageNextEl = document.getElementById("prediction-page-next");
  const predPageLabelEl = document.getElementById("prediction-page-label");

  const annotationMessageEl = document.getElementById("annotation-results-message");
  const annotationSummaryEl = document.getElementById("annotation-results-summary");
  const annotationDiagnosticsMessageEl = document.getElementById("annotation-diagnostics-message");
  const annotationDiagnosticsTableEl = document.getElementById("annotation-diagnostics-table");
  const annotationDiagnosticsBodyEl = document.getElementById("annotation-diagnostics-body");
  const annotationEvidenceFilterEl = document.getElementById("annotation-evidence-classification-filter");
  const annotationEvidenceOutcomeEl = document.getElementById("annotation-evidence-outcome-filter");
  const annotationVcfMessageEl = document.getElementById("annotation-vcf-message");
  const annotationVcfTableEl = document.getElementById("annotation-vcf-table");
  const annotationVcfHeadRowEl = document.getElementById("annotation-vcf-head-row");
  const annotationVcfBodyEl = document.getElementById("annotation-vcf-body");
  const reportingMessageEl = document.getElementById("reporting-results-message");
  const reportingSummaryEl = document.getElementById("reporting-results-summary");
  const reportingSignificantEl = document.getElementById("reporting-significant-results");
  const reportingEvidenceDiagnosticsEl = document.getElementById("reporting-evidence-diagnostics");
  const summaryMessageEl = document.getElementById("variant-summary-message");
  const summaryTableEl = document.getElementById("variant-summary-table");
  const summaryBodyEl = document.getElementById("variant-summary-body");
  const summaryCompletenessFilterEl = document.getElementById("variant-summary-completeness-filter");
  const summaryPagePrevEl = document.getElementById("variant-summary-page-prev");
  const summaryPageNextEl = document.getElementById("variant-summary-page-next");
  const summaryPageLabelEl = document.getElementById("variant-summary-page-label");

  const finalHtmlArtifactsMessageEl = document.getElementById("final-html-artifacts-message");
  const finalHtmlArtifactsEl = document.getElementById("final-html-artifacts");

  const classificationArtifactsMessageEl = document.getElementById("classification-artifacts-message");
  const classificationInputVcfMessageEl = document.getElementById("classification-input-vcf-message");
  const classificationInputVcfTableEl = document.getElementById("classification-input-vcf-table");
  const classificationInputVcfHeadRowEl = document.getElementById("classification-input-vcf-head-row");
  const classificationInputVcfBodyEl = document.getElementById("classification-input-vcf-body");
  const classificationInputVcfPosEl = document.getElementById("classification-input-vcf-pos");
  const classificationInputVcfSearchEl = document.getElementById("classification-input-vcf-search");
  const classificationInputVcfClearEl = document.getElementById("classification-input-vcf-clear");
  const classificationInputVcfPagePrevEl = document.getElementById("classification-input-vcf-page-prev");
  const classificationInputVcfPageNextEl = document.getElementById("classification-input-vcf-page-next");
  const classificationInputVcfPageLabelEl = document.getElementById("classification-input-vcf-page-label");

  const predictionArtifactsMessageEl = document.getElementById("prediction-artifacts-message");
  const predictionInputVcfMessageEl = document.getElementById("prediction-input-vcf-message");
  const predictionInputVcfTableEl = document.getElementById("prediction-input-vcf-table");
  const predictionInputVcfHeadRowEl = document.getElementById("prediction-input-vcf-head-row");
  const predictionInputVcfBodyEl = document.getElementById("prediction-input-vcf-body");
  const predictionInputVcfPosEl = document.getElementById("prediction-input-vcf-pos");
  const predictionInputVcfSearchEl = document.getElementById("prediction-input-vcf-search");
  const predictionInputVcfClearEl = document.getElementById("prediction-input-vcf-clear");
  const predictionInputVcfPagePrevEl = document.getElementById("prediction-input-vcf-page-prev");
  const predictionInputVcfPageNextEl = document.getElementById("prediction-input-vcf-page-next");
  const predictionInputVcfPageLabelEl = document.getElementById("prediction-input-vcf-page-label");

  const annotationVcfPosEl = document.getElementById("annotation-vcf-pos");
  const annotationVcfSearchEl = document.getElementById("annotation-vcf-search");
  const annotationVcfClearEl = document.getElementById("annotation-vcf-clear");
  const annotationVcfPagePrevEl = document.getElementById("annotation-vcf-page-prev");
  const annotationVcfPageNextEl = document.getElementById("annotation-vcf-page-next");
  const annotationVcfPageLabelEl = document.getElementById("annotation-vcf-page-label");

  const annotationDbsnpMessageEl = document.getElementById("annotation-dbsnp-message");
  const annotationDbsnpTableEl = document.getElementById("annotation-dbsnp-table");
  const annotationDbsnpBodyEl = document.getElementById("annotation-dbsnp-body");
  const annotationClinvarMessageEl = document.getElementById("annotation-clinvar-message");
  const annotationClinvarTableEl = document.getElementById("annotation-clinvar-table");
  const annotationClinvarBodyEl = document.getElementById("annotation-clinvar-body");
  const annotationGnomadMessageEl = document.getElementById("annotation-gnomad-message");
  const annotationGnomadTableEl = document.getElementById("annotation-gnomad-table");
  const annotationGnomadBodyEl = document.getElementById("annotation-gnomad-body");

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

  const evidenceMessageEl = document.getElementById("variant-evidence-message");
  const evDbsnpOutcomeEl = document.getElementById("variant-ev-dbsnp-outcome");
  const evDbsnpCompletenessEl = document.getElementById("variant-ev-dbsnp-completeness");
  const evDbsnpRsidEl = document.getElementById("variant-ev-dbsnp-rsid");
  const evDbsnpReasonEl = document.getElementById("variant-ev-dbsnp-reason");
  const evDbsnpTimestampEl = document.getElementById("variant-ev-dbsnp-timestamp");
  const evDbsnpSourceEl = document.getElementById("variant-ev-dbsnp-source");

  const evClinvarOutcomeEl = document.getElementById("variant-ev-clinvar-outcome");
  const evClinvarCompletenessEl = document.getElementById("variant-ev-clinvar-completeness");
  const evClinvarIdEl = document.getElementById("variant-ev-clinvar-id");
  const evClinvarSignificanceEl = document.getElementById("variant-ev-clinvar-significance");
  const evClinvarReasonEl = document.getElementById("variant-ev-clinvar-reason");
  const evClinvarTimestampEl = document.getElementById("variant-ev-clinvar-timestamp");
  const evClinvarSourceEl = document.getElementById("variant-ev-clinvar-source");

  const evGnomadOutcomeEl = document.getElementById("variant-ev-gnomad-outcome");
  const evGnomadCompletenessEl = document.getElementById("variant-ev-gnomad-completeness");
  const evGnomadIdEl = document.getElementById("variant-ev-gnomad-id");
  const evGnomadAfEl = document.getElementById("variant-ev-gnomad-af");
  const evGnomadReasonEl = document.getElementById("variant-ev-gnomad-reason");
  const evGnomadTimestampEl = document.getElementById("variant-ev-gnomad-timestamp");
  const evGnomadSourceEl = document.getElementById("variant-ev-gnomad-source");

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
    !annotationDiagnosticsMessageEl ||
    !annotationDiagnosticsTableEl ||
    !annotationDiagnosticsBodyEl ||
    !annotationVcfMessageEl ||
    !annotationVcfTableEl ||
    !annotationVcfHeadRowEl ||
    !annotationVcfBodyEl ||
    !reportingMessageEl ||
    !reportingSummaryEl ||
    !summaryMessageEl ||
    !summaryTableEl ||
    !summaryBodyEl
  ) {
    return;
  }

  const STORAGE_KEY = "sp_current_run";
  let inFlight = false;
  let refreshTimerId = null;
  let pausePolling = false;
  let lastTriggerEl = null;
  let cachedPredictionRows = [];
  let latestStages = [];
  let variantDetailsRequestSeq = 0;
  let lastRunId = loadRunId();
  let pendingVariantRefreshStage = null;
  const PRE_PAGE_SIZE = 200;
  const CLS_PAGE_SIZE = 200;
  const PRED_PAGE_SIZE = 200;
  const SUMMARY_PAGE_SIZE = 200;
  const VCF_PREVIEW_PAGE_SIZE = 200;
  const ARTIFACT_REFRESH_MS = 5000;
  const ARTIFACT_PREVIEW_LIMIT = 200;
  const EVIDENCE_REFRESH_MS = 5000;
  const EVIDENCE_LIMIT = 200;
  const JSON_PREVIEW_MAX_CHARS = 240;
  let prePage = 0;
  let preTotalCount = 0;
  let clsPage = 0;
  let clsTotalCount = 0;
  let predPage = 0;
  let predTotalCount = 0;
  let summaryPage = 0;
  let summaryTotalCount = 0;
  let clsInputVcfPage = 0;
  let clsInputVcfHasNext = false;
  let clsInputVcfPosFilter = "";
  let predInputVcfPage = 0;
  let predInputVcfHasNext = false;
  let predInputVcfPosFilter = "";
  let annotationVcfPage = 0;
  let annotationVcfHasNext = false;
  let annotationVcfPosFilter = "";
  let lastArtifactsFetchedAt = 0;
  let lastEvidenceFetchedAt = 0;
  let htmlArtifactsReady = false;
  const dateTimeFormatter = new Intl.DateTimeFormat("en-US", {
    month: "2-digit",
    day: "2-digit",
    year: "numeric",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
  });

  function clearEl(el) {
    while (el.firstChild) el.removeChild(el.firstChild);
  }

  function updatePager(totalCount, pageSize, pageIndex, prevEl, nextEl, labelEl) {
    if (!labelEl) return;
    const total = Math.max(0, Number.isFinite(totalCount) ? totalCount : 0);
    const size = Math.max(1, Number.isFinite(pageSize) ? pageSize : 1);
    const totalPages = total > 0 ? Math.ceil(total / size) : 0;
    const currentPage = totalPages > 0 ? Math.min(pageIndex + 1, totalPages) : 0;
    if (prevEl) prevEl.disabled = currentPage <= 1;
    if (nextEl) nextEl.disabled = totalPages > 0 ? currentPage >= totalPages : true;
    labelEl.textContent =
      totalPages > 0 ? `Page ${currentPage} of ${totalPages} (${total} rows)` : "No rows";
  }

  function updateSimplePager(pageIndex, hasNext, prevEl, nextEl, labelEl, note) {
    if (!labelEl) return;
    const currentPage = Math.max(0, Number.isFinite(pageIndex) ? pageIndex : 0) + 1;
    if (prevEl) prevEl.disabled = currentPage <= 1;
    if (nextEl) nextEl.disabled = !hasNext;
    if (!note && currentPage === 1 && !hasNext) {
      labelEl.textContent = "No rows";
      return;
    }
    labelEl.textContent = note ? `Page ${currentPage} (${note})` : `Page ${currentPage}`;
  }

  function normalizePosFilter(value) {
    const text = String(value || "").trim();
    if (!text) return "";
    const parsed = Number(text);
    if (!Number.isFinite(parsed) || parsed <= 0) return "";
    return String(Math.floor(parsed));
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

  const STATUS_LABEL_OVERRIDES = {
    queued: "Queued",
    pending: "Pending",
    running: "Running",
    succeeded: "Succeeded",
    failed: "Failed",
    canceled: "Canceled",
    partial: "Partial",
    unavailable: "Unavailable",
    available: "Available",
    "not available": "Not available",
    "not applicable": "Not applicable",
    ok: "OK",
    error: "Error",
    disabled: "Disabled",
    complete: "Complete",
    found: "Found",
    "not found": "Not found",
    computed: "Computed",
    "not computed": "Not computed",
    skipped: "Skipped",
  };
  const STATUS_ICON_MAP = {
    queued: "[~]",
    pending: "[~]",
    running: "[~]",
    partial: "[~]",
    "not applicable": "[~]",
    skipped: "[~]",
    succeeded: "[OK]",
    complete: "[OK]",
    ok: "[OK]",
    available: "[OK]",
    found: "[OK]",
    computed: "[OK]",
    failed: "[!]",
    error: "[!]",
    unavailable: "[!]",
    "not available": "[!]",
    "not found": "[!]",
    "not computed": "[!]",
    canceled: "[x]",
    disabled: "[x]",
  };

  function normalizeStatusKey(value) {
    if (!value) return "";
    return String(value).trim().toLowerCase().replace(/_/g, " ");
  }

  function statusTooltipKey(status) {
    const normalized = normalizeStatusKey(status);
    if (!normalized) return "";
    return `status.${normalized.replace(/\s+/g, "_")}`;
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
    const indicator = document.createElement("button");
    indicator.className = "status-indicator";
    indicator.type = "button";
    const normalized = normalizeStatusKey(status);
    if (normalized) {
      indicator.dataset.status = normalized.replace(/\s+/g, "-");
      const tooltipKey = statusTooltipKey(normalized);
      if (tooltipKey) indicator.dataset.tooltipKey = tooltipKey;
    }

    const icon = statusIcon(status);
    if (icon) {
      const iconSpan = document.createElement("span");
      iconSpan.className = "status-icon";
      iconSpan.setAttribute("aria-hidden", "true");
      iconSpan.textContent = icon;
      indicator.appendChild(iconSpan);
    }

    const label =
      labelText || statusLabel(status) || (status && String(status).trim() ? String(status) : "\u2014");
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
    window.SPTooltips?.applyGlossary?.(container);
  }

  function refreshTooltips(scope) {
    window.SPTooltips?.applyGlossary?.(scope || document);
  }

  function isInteractiveElement(target) {
    if (!target || typeof target.closest !== "function") return false;
    return Boolean(target.closest("button, a, input, select, textarea, label"));
  }

  function wireVariantRowSelection(row, tr) {
    if (!offcanvasEl || !row?.variant_id || !tr) return;
    const variantKey = row?.variant_key ? String(row.variant_key) : "variant";
    tr.tabIndex = 0;
    tr.setAttribute("aria-controls", "variant-details-offcanvas");
    tr.setAttribute("aria-expanded", "false");
    tr.setAttribute("role", "button");
    tr.setAttribute("aria-label", `Open details for ${variantKey}`);
    tr.addEventListener("click", (event) => {
      if (isInteractiveElement(event?.target)) return;
      void openVariantDetails(row, tr);
    });
    tr.addEventListener("keydown", (event) => {
      if (isInteractiveElement(event?.target)) return;
      if (event.key === "Enter" || event.key === " ") {
        event.preventDefault();
        void openVariantDetails(row, tr);
      }
    });
  }

  function formatDateTime(value) {
    if (!value) return "\u2014";
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return String(value);
    return dateTimeFormatter.format(d);
  }

  function hideTable(tableEl, bodyEl) {
    tableEl.hidden = true;
    clearEl(bodyEl);
  }

  function hideVcfTable(tableEl, headRowEl, bodyEl) {
    if (!tableEl || !headRowEl || !bodyEl) return;
    tableEl.hidden = true;
    clearEl(headRowEl);
    clearEl(bodyEl);
  }

  function hideJsonlTable(tableEl, bodyEl) {
    if (!tableEl || !bodyEl) return;
    tableEl.hidden = true;
    clearEl(bodyEl);
  }

  function hideAnnotationVcfTable() {
    hideVcfTable(annotationVcfTableEl, annotationVcfHeadRowEl, annotationVcfBodyEl);
  }

  function hideAnnotationDiagnosticsTable() {
    annotationDiagnosticsTableEl.hidden = true;
    clearEl(annotationDiagnosticsBodyEl);
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

  function normalizeVcfHeaderName(name) {
    if (!name) return "";
    return String(name).replace(/^#/, "").trim().toUpperCase();
  }

  function selectVcfColumns(headerCols, preferred) {
    if (!Array.isArray(headerCols)) return [];
    if (!Array.isArray(preferred) || preferred.length === 0) {
      return headerCols.map((_, idx) => idx);
    }
    const normalized = headerCols.map((col) => normalizeVcfHeaderName(col));
    const indices = [];
    for (const col of preferred) {
      const idx = normalized.indexOf(String(col).toUpperCase());
      if (idx >= 0 && !indices.includes(idx)) indices.push(idx);
    }
    return indices.length > 0 ? indices : headerCols.map((_, idx) => idx);
  }

  function renderVcfTable(parsed, tableEl, headRowEl, bodyEl, options = {}) {
    if (!tableEl || !headRowEl || !bodyEl) return;
    tableEl.hidden = true;
    clearEl(headRowEl);
    clearEl(bodyEl);
    if (!parsed) return;

    const headerCols = Array.isArray(parsed.headerCols) ? parsed.headerCols : [];
    const indices = selectVcfColumns(headerCols, options.columns || []);

    for (const idx of indices) {
      const th = document.createElement("th");
      th.scope = "col";
      th.textContent = headerCols[idx] || "\u2014";
      headRowEl.appendChild(th);
    }

    const expectedCols = headerCols.length;
    for (const rawRow of parsed.dataRows || []) {
      const tr = document.createElement("tr");
      const row = Array.isArray(rawRow) ? rawRow.slice() : [];
      if (row.length > expectedCols && expectedCols > 0) {
        const collapsed = row.slice(expectedCols - 1).join("\t");
        row.length = expectedCols - 1;
        row.push(collapsed);
      }

      for (const idx of indices) {
        const td = document.createElement("td");
        td.textContent = idx < row.length ? String(row[idx] ?? "") : "";
        tr.appendChild(td);
      }
      bodyEl.appendChild(tr);
    }

    tableEl.hidden = false;
  }

  function formatVcfPreviewMessage(posFilter, rowCount, truncated) {
    if (posFilter) {
      if (rowCount === 0) return `No rows match position ${posFilter}.`;
      if (truncated) return `Showing first ${rowCount} matches for position ${posFilter}.`;
      return `Showing ${rowCount} matches for position ${posFilter}.`;
    }
    if (rowCount === 0) return "Artifact preview is empty.";
    if (truncated) return `Showing ${rowCount} rows (more available).`;
    return `Showing ${rowCount} rows.`;
  }

  async function refreshArtifactVcfPreview({
    runId,
    artifactsByName,
    artifactName,
    messageEl,
    tableEl,
    headRowEl,
    bodyEl,
    pageIndex,
    posFilter,
    prevEl,
    nextEl,
    labelEl,
    setHasNext,
  }) {
    const artifact = artifactsByName.get(artifactName);
    const reasonMessage = artifactReasonMessage(artifact, artifactName, artifact?.stage || "pipeline");
    if (!artifact || !artifact.available) {
      setMessage(messageEl, reasonMessage);
      hideVcfTable(tableEl, headRowEl, bodyEl);
      if (setHasNext) setHasNext(false);
      updateSimplePager(0, false, prevEl, nextEl, labelEl, "");
      return;
    }

    try {
      const { resp, payload } = await getJson(
        buildArtifactVcfPreviewUrl(runId, artifactName, pageIndex, posFilter),
      );
      if (!resp.ok || !payload?.ok) {
        setMessage(messageEl, payload?.error?.message || "Unable to load artifact preview.");
        hideVcfTable(tableEl, headRowEl, bodyEl);
        if (setHasNext) setHasNext(false);
        updateSimplePager(0, false, prevEl, nextEl, labelEl, "");
        return;
      }
      const artifactData = payload?.data?.artifact;
      if (!artifactData?.available) {
        setMessage(messageEl, "Artifact preview not available.");
        hideVcfTable(tableEl, headRowEl, bodyEl);
        if (setHasNext) setHasNext(false);
        updateSimplePager(0, false, prevEl, nextEl, labelEl, "");
        return;
      }
      const lines = Array.isArray(artifactData?.preview_lines) ? artifactData.preview_lines : [];
      if (lines.length === 0) {
        setMessage(messageEl, formatVcfPreviewMessage(posFilter, 0, false));
        hideVcfTable(tableEl, headRowEl, bodyEl);
        if (setHasNext) setHasNext(false);
        updateSimplePager(0, false, prevEl, nextEl, labelEl, "");
        return;
      }
      const parsed = parseVcfPreviewLines(lines);
      if (!parsed) {
        setMessage(messageEl, "Artifact preview could not be parsed as VCF.");
        hideVcfTable(tableEl, headRowEl, bodyEl);
        if (setHasNext) setHasNext(false);
        updateSimplePager(0, false, prevEl, nextEl, labelEl, "");
        return;
      }

      const rowCount = Number(artifactData?.data_line_count ?? parsed.dataRows.length ?? 0);
      const truncated = Boolean(artifactData?.truncated);
      const hasNext = !posFilter && truncated;
      if (setHasNext) setHasNext(hasNext);
      const note = posFilter ? `search: ${rowCount} matches` : `${rowCount} rows`;
      updateSimplePager(pageIndex, hasNext, prevEl, nextEl, labelEl, note);
      setMessage(messageEl, formatVcfPreviewMessage(posFilter, rowCount, truncated));
      renderVcfTable(parsed, tableEl, headRowEl, bodyEl, {});
    } catch {
      setMessage(messageEl, "Unable to load artifact preview.");
      hideVcfTable(tableEl, headRowEl, bodyEl);
      if (setHasNext) setHasNext(false);
      updateSimplePager(0, false, prevEl, nextEl, labelEl, "");
    }
  }

  function renderAnnotationVcfTable(parsed) {
    hideAnnotationVcfTable();
    if (!parsed) return;
    renderVcfTable(parsed, annotationVcfTableEl, annotationVcfHeadRowEl, annotationVcfBodyEl, {});
  }

  function basename(path) {
    if (!path) return "";
    const asText = String(path);
    const parts = asText.split(/[/\\\\]+/);
    return parts[parts.length - 1] || asText;
  }

  function truncateText(text, maxLen) {
    const raw = text == null ? "" : String(text);
    if (!maxLen || raw.length <= maxLen) return raw;
    return `${raw.slice(0, Math.max(0, maxLen - 1))}\u2026`;
  }

  function jsonlVariantLabel(row) {
    if (!row || typeof row !== "object") return "\u2014";
    if (row.variant_key) return String(row.variant_key);
    if (row.input) return String(row.input);
    if (row.variant) return String(row.variant);
    if (row.id) return String(row.id);
    if (row.location && row.allele_string) return `${row.location} ${row.allele_string}`;
    if (row.location) return String(row.location);
    if (row.seq_region_name && row.start && row.allele_string) {
      return `${row.seq_region_name}:${row.start} ${row.allele_string}`;
    }
    if (row.chrom && row.pos && row.ref && row.alt) {
      return `${row.chrom}:${row.pos}:${row.ref}>${row.alt}`;
    }
    return "\u2014";
  }

  function jsonlSummaryText(row) {
    if (!row || typeof row !== "object") return "\u2014";
    const parts = [];
    const consequence =
      row.most_severe_consequence
      || row.primary_consequence
      || row.consequence
      || row.consequence_terms;
    if (consequence) {
      const value = Array.isArray(consequence) ? consequence.join(", ") : String(consequence);
      parts.push(value);
    }
    const impact = row.impact || row.severity;
    if (impact) parts.push(`impact: ${impact}`);
    const gene = row.symbol || row.gene_symbol || row.gene;
    if (gene) parts.push(`gene: ${gene}`);
    const clinical = row.clinical_significance || row.clinical_significance_summary;
    if (clinical) parts.push(`clin_sig: ${clinical}`);
    if (parts.length === 0) {
      try {
        return truncateText(JSON.stringify(row), 140);
      } catch {
        return "\u2014";
      }
    }
    return truncateText(parts.join(" | "), 220);
  }

  function renderJsonlRecordTable(rows, tableEl, bodyEl) {
    if (!tableEl || !bodyEl) return;
    clearEl(bodyEl);
    if (!rows || rows.length === 0) {
      tableEl.hidden = true;
      return;
    }
    for (const row of rows) {
      const tr = document.createElement("tr");
      const td = document.createElement("td");
      let text = "";
      try {
        text = JSON.stringify(row);
      } catch {
        text = String(row ?? "");
      }
      td.textContent = truncateText(text, JSON_PREVIEW_MAX_CHARS);
      tr.appendChild(td);
      bodyEl.appendChild(tr);
    }
    tableEl.hidden = false;
  }

  function renderJsonlSummaryTable(rows, tableEl, bodyEl) {
    if (!tableEl || !bodyEl) return;
    clearEl(bodyEl);
    if (!rows || rows.length === 0) {
      tableEl.hidden = true;
      return;
    }
    for (const row of rows) {
      const tr = document.createElement("tr");
      const variantTd = document.createElement("td");
      variantTd.className = "text-nowrap";
      variantTd.textContent = jsonlVariantLabel(row);
      tr.appendChild(variantTd);

      const summaryTd = document.createElement("td");
      summaryTd.textContent = jsonlSummaryText(row);
      tr.appendChild(summaryTd);

      bodyEl.appendChild(tr);
    }
    tableEl.hidden = false;
  }

  function setSummaryRows(containerEl, rows) {
    if (!containerEl) return;
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

  function formatSummaryValue(value) {
    if (Number.isFinite(value)) return String(value);
    if (typeof value === "string" && value.trim()) return value;
    return "\u2014";
  }

  function formatCategoryCounts(counts) {
    if (!counts || typeof counts !== "object") return "\u2014";
    const entries = Object.entries(counts).filter(([, value]) => Number.isFinite(value));
    if (entries.length === 0) return "\u2014";
    return entries.map(([key, value]) => `${key}: ${value}`).join(", ");
  }

  function buildStageSummary(stageName, valueBuilder) {
    const stage = stageByName(latestStages, stageName);
    if (!stage) return "Status: queued";
    if (stage.status !== "succeeded") return `Status: ${stage.status || "queued"}`;
    return valueBuilder(stage?.stats ?? {});
  }

  function buildEvidenceDiagnosticsRows(stats, sourceCompletenessRaw, sourceCompletenessReasonRaw) {
    const diagnosticSources = [
      { key: "dbsnp", label: "dbSNP" },
      { key: "clinvar", label: "ClinVar" },
      { key: "gnomad", label: "gnomAD" },
    ];

    return diagnosticSources.map((source) => {
      const enabled = stats?.[`${source.key}_enabled`] !== false;
      const errors = Number(stats?.[`${source.key}_errors`] ?? 0);
      const retryAttempts = Number(stats?.[`${source.key}_retry_attempts`] ?? 0);
      const normalizedCompleteness = normalizeCompleteness(sourceCompletenessRaw?.[source.key]);
      const completenessDisplay = formatCompleteness(normalizedCompleteness) || "\u2014";
      const completenessReason = sourceCompletenessReasonRaw?.[source.key] || "";
      const completenessImpact = completenessReason
        ? `${completenessDisplay} (${completenessReason})`
        : completenessDisplay;
      let outcome = "ok";
      if (!enabled) {
        outcome = "disabled";
      } else if (errors > 0) {
        outcome = "error";
      } else if (normalizedCompleteness) {
        outcome = normalizedCompleteness;
      }

      const reasonCountsRaw = stats?.[`${source.key}_error_reason_counts`];
      const reasonCounts = reasonCountsRaw && typeof reasonCountsRaw === "object" ? reasonCountsRaw : {};
      const topReasons = Object.entries(reasonCounts)
        .map(([reason, count]) => ({ reason, count: Number(count || 0) }))
        .filter((entry) => Number.isFinite(entry.count) && entry.count > 0)
        .sort((a, b) => b.count - a.count)
        .slice(0, 3)
        .map((entry) => `${entry.reason}: ${entry.count}`);
      const statusCountsRaw = stats?.[`${source.key}_error_http_status_counts`];
      const statusCounts = statusCountsRaw && typeof statusCountsRaw === "object" ? statusCountsRaw : {};
      const httpStatuses = Object.entries(statusCounts)
        .map(([status, count]) => ({ status, count: Number(count || 0) }))
        .filter((entry) => Number.isFinite(entry.count) && entry.count > 0)
        .sort((a, b) => b.count - a.count)
        .slice(0, 4)
        .map((entry) => `${entry.status}: ${entry.count}`);

      const details = stats?.[`${source.key}_error_details`];
      const detailsObj = details && typeof details === "object" ? details : {};
      const errorList = Array.isArray(detailsObj?.errors) ? detailsObj.errors : [];
      const firstError = errorList[0] || null;
      let errorSample = "\u2014";
      if (firstError) {
        const code = firstError.reason_code || "UNKNOWN_ERROR";
        const message = firstError.reason_message ? String(firstError.reason_message) : "";
        errorSample = message ? `${code}: ${message}` : code;
      }
      const hint = detailsObj?.hint || "";
      const note = stats?.[`${source.key}_note`] || "";

      return {
        source: source.label,
        enabled: enabled ? "Yes" : "No",
        outcome,
        completenessKey: normalizedCompleteness || "",
        completenessImpact,
        errors,
        retryAttempts,
        errorSample,
        topReasons: topReasons.join(", "),
        httpStatuses: httpStatuses.join(", "),
        hint: hint || note || "\u2014",
      };
    });
  }

  function loadStoredRun() {
    try {
      return JSON.parse(localStorage.getItem(STORAGE_KEY));
    } catch {
      return null;
    }
  }

  function loadRunId() {
    const stored = loadStoredRun();
    return stored?.run_id ?? null;
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

  function formatEvidencePolicy(policy) {
    const normalized = String(policy || "").trim().toLowerCase();
    if (normalized === "stop") return "stop (fail annotation stage)";
    if (normalized === "continue") return "continue (allow partial evidence)";
    return "";
  }

  function normalizeCompleteness(value) {
    const normalized = String(value || "").trim().toLowerCase();
    if (normalized === "complete" || normalized === "partial" || normalized === "unavailable") {
      return normalized;
    }
    return "";
  }

  function formatCompleteness(value) {
    const normalized = normalizeCompleteness(value);
    if (!normalized) return "";
    if (normalized === "complete") return "complete";
    if (normalized === "partial") return "partial";
    return "unavailable";
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

  function isTerminalPipelineSnapshot(stages) {
    if (!Array.isArray(stages) || stages.length === 0) return false;
    const hasRunning = stages.some((stage) => stage?.status === "running");
    if (hasRunning) return false;
    const hasFailedOrCanceled = stages.some((stage) => {
      const status = stage?.status;
      return status === "failed" || status === "canceled";
    });
    if (hasFailedOrCanceled) return true;
    const reporting = stageByName(stages, "reporting");
    return reporting?.status === "succeeded";
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
    latestStages = [];
    prePage = 0;
    preTotalCount = 0;
    clsPage = 0;
    clsTotalCount = 0;
    predPage = 0;
    predTotalCount = 0;
    summaryPage = 0;
    summaryTotalCount = 0;
    clsInputVcfPage = 0;
    clsInputVcfHasNext = false;
    clsInputVcfPosFilter = "";
    predInputVcfPage = 0;
    predInputVcfHasNext = false;
    predInputVcfPosFilter = "";
    annotationVcfPage = 0;
    annotationVcfHasNext = false;
    annotationVcfPosFilter = "";
    if (classificationInputVcfPosEl) classificationInputVcfPosEl.value = "";
    if (predictionInputVcfPosEl) predictionInputVcfPosEl.value = "";
    if (annotationVcfPosEl) annotationVcfPosEl.value = "";
    if (clsFilterEl) clsFilterEl.value = "";
    if (summaryCompletenessFilterEl) summaryCompletenessFilterEl.value = "";
    if (annotationEvidenceFilterEl) annotationEvidenceFilterEl.value = "missense";
    if (annotationEvidenceOutcomeEl) annotationEvidenceOutcomeEl.value = "all";
    clearDetails();
    setMessage(finalMessageEl, "Choose a VCF file and press Start.");
    setMessage(parserMessageEl, "Parser results will appear after run start.");
    setSummaryRows(parserSummaryEl, []);
    setMessage(preMessageEl, "Pre-annotation results will appear after stage completion.");
    hideTable(preTableEl, preBodyEl);
    updatePager(preTotalCount, PRE_PAGE_SIZE, prePage, prePagePrevEl, prePageNextEl, prePageLabelEl);
    setMessage(clsMessageEl, "Classification results will appear after stage completion.");
    hideTable(clsTableEl, clsBodyEl);
    updatePager(clsTotalCount, CLS_PAGE_SIZE, clsPage, clsPagePrevEl, clsPageNextEl, clsPageLabelEl);
    setMessage(predMessageEl, "Prediction results will appear after stage completion.");
    hideTable(predTableEl, predBodyEl);
    updatePager(predTotalCount, PRED_PAGE_SIZE, predPage, predPagePrevEl, predPageNextEl, predPageLabelEl);
    setMessage(annotationMessageEl, "Annotation technical outputs will appear after stage completion.");
    setSummaryRows(annotationSummaryEl, []);
    setMessage(annotationDiagnosticsMessageEl, "Evidence diagnostics will appear after stage completion.");
    hideAnnotationDiagnosticsTable();
    setMessage(annotationVcfMessageEl, "Annotated VCF preview will appear after stage completion.");
    hideAnnotationVcfTable();
    setMessage(finalHtmlArtifactsMessageEl, "HTML summaries will appear after stage completion.");
    clearEl(finalHtmlArtifactsEl);
    setMessage(reportingMessageEl, "Reporting summary will appear here after stage completion.");
    setSummaryRows(reportingSummaryEl, []);
    setSummaryRows(reportingSignificantEl, []);
    setSummaryRows(reportingEvidenceDiagnosticsEl, []);
    setMessage(summaryMessageEl, "Variant summary will appear after parser completion.");
    hideTable(summaryTableEl, summaryBodyEl);
    updatePager(
      summaryTotalCount,
      SUMMARY_PAGE_SIZE,
      summaryPage,
      summaryPagePrevEl,
      summaryPageNextEl,
      summaryPageLabelEl,
    );
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

  function refreshArtifactsForCurrentRun() {
    const runId = loadRunId();
    if (!runId) return;
    lastArtifactsFetchedAt = 0;
    void refreshArtifacts(runId);
  }

  function refreshAnnotationVcfForCurrentRun() {
    const runId = loadRunId();
    if (!runId) return;
    const stage = stageByName(latestStages, "annotation");
    void refreshAnnotationOutput(runId, stage);
  }

  function buildArtifactVcfPreviewUrl(runId, name, pageIndex, posFilter) {
    const params = new URLSearchParams();
    params.set("name", name);
    params.set("limit", String(VCF_PREVIEW_PAGE_SIZE));
    if (posFilter) {
      params.set("pos", posFilter);
    } else {
      params.set("offset", String(pageIndex * VCF_PREVIEW_PAGE_SIZE));
    }
    return `/api/v1/runs/${encodeURIComponent(runId)}/artifacts/preview?${params.toString()}`;
  }

  function buildAnnotationVcfPreviewUrl(runId, pageIndex, posFilter) {
    const params = new URLSearchParams();
    params.set("limit", String(VCF_PREVIEW_PAGE_SIZE));
    if (posFilter) {
      params.set("pos", posFilter);
    } else {
      params.set("offset", String(pageIndex * VCF_PREVIEW_PAGE_SIZE));
    }
    return `/api/v1/runs/${encodeURIComponent(runId)}/annotation_output?${params.toString()}`;
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
      { label: "Lines read", value: stats?.lines_read },
      { label: "Records seen", value: stats?.records_seen },
      { label: "SNV records created", value: stats?.snv_records_created },
      { label: "SNVs persisted", value: stats?.snv_records_persisted },
      { label: "Non-SNV alleles skipped", value: stats?.non_snv_alleles_skipped },
      { label: "Multi-ALT rows", value: stats?.multi_alt_rows_seen },
      { label: "Duplicates ignored", value: stats?.duplicate_records_ignored },
    ]);
  }

  function evidenceSourcesLabel(row) {
    const sources = [];
    if (row?.has_dbsnp) sources.push("dbSNP");
    if (row?.has_clinvar) sources.push("ClinVar");
    if (row?.has_gnomad) sources.push("gnomAD");
    return sources.length > 0 ? sources.join(", ") : "";
  }

  function predictionAvailability(row, predictionStage) {
    const category = (row?.consequence_category || "").toLowerCase();
    if (category && category !== "missense") {
      return { status: "not applicable", label: statusLabel("not applicable") };
    }
    if (row?.has_prediction) return { status: "available", label: statusLabel("available") };
    if (predictionStage?.status === "succeeded") {
      return { status: "not available", label: statusLabel("not available") };
    }
    if (predictionStage?.status === "failed") return { status: "failed", label: statusLabel("failed") };
    if (predictionStage?.status === "running") return { status: "running", label: statusLabel("running") };
    return { status: "pending", label: statusLabel("pending") };
  }

  function evidenceAvailability(row, annotationStage) {
    const label = evidenceSourcesLabel(row);
    if (label) {
      return { status: "available", label: `Available (${label})` };
    }
    if (annotationStage?.status === "succeeded") {
      return { status: "not available", label: statusLabel("not available") };
    }
    if (annotationStage?.status === "failed") return { status: "failed", label: statusLabel("failed") };
    if (annotationStage?.status === "running") return { status: "running", label: statusLabel("running") };
    return { status: "pending", label: statusLabel("pending") };
  }

  const COMPLETENESS_FAILURE_STAGES = new Set([
    "parser",
    "classification",
    "prediction",
    "annotation",
  ]);

  function variantCompleteness(row) {
    const failedStage = Array.isArray(latestStages)
      ? latestStages.find(
        (stage) =>
          stage?.status === "failed" && COMPLETENESS_FAILURE_STAGES.has(stage?.stage_name),
      )
      : null;
    if (failedStage) return "failed";

    const parserStage = stageByName(latestStages, "parser");
    if (parserStage?.status !== "succeeded") return "partial";

    const classificationStage = stageByName(latestStages, "classification");
    if (classificationStage?.status !== "succeeded") return "partial";
    if (!row?.consequence_category) return "partial";

    const predictionStage = stageByName(latestStages, "prediction");
    const category = (row?.consequence_category || "").toLowerCase();
    if (category === "missense" && predictionStage?.status !== "succeeded") return "partial";

    const annotationStage = stageByName(latestStages, "annotation");
    if (annotationStage?.status !== "succeeded") return "partial";
    const completeness = normalizeCompleteness(annotationStage?.stats?.annotation_evidence_completeness);
    if (completeness === "unavailable") return "unavailable";
    if (completeness === "partial") return "partial";

    return "complete";
  }

  function renderVariantSummaryRows(rows) {
    clearEl(summaryBodyEl);
    const predictionStage = stageByName(latestStages, "prediction");
    const annotationStage = stageByName(latestStages, "annotation");

    for (const row of rows) {
      const tr = document.createElement("tr");

      const variantCell = document.createElement("td");
      variantCell.className = "text-nowrap";
      variantCell.textContent = row?.variant_key ?? "\u2014";
      tr.appendChild(variantCell);

      const chromCell = document.createElement("td");
      chromCell.className = "text-nowrap";
      chromCell.textContent = row?.chrom ?? "\u2014";
      tr.appendChild(chromCell);

      const posCell = document.createElement("td");
      posCell.className = "text-nowrap";
      posCell.textContent = row?.pos != null ? String(row.pos) : "\u2014";
      tr.appendChild(posCell);

      const refCell = document.createElement("td");
      refCell.className = "text-nowrap";
      refCell.textContent = row?.ref ?? "\u2014";
      tr.appendChild(refCell);

      const altCell = document.createElement("td");
      altCell.className = "text-nowrap";
      altCell.textContent = row?.alt ?? "\u2014";
      tr.appendChild(altCell);

      const classificationCell = document.createElement("td");
      classificationCell.className = "text-nowrap";
      classificationCell.textContent = row?.consequence_category ?? "pending";
      tr.appendChild(classificationCell);

      const predictionCell = document.createElement("td");
      predictionCell.className = "text-nowrap";
      const predictionInfo = predictionAvailability(row, predictionStage);
      setStatusIndicator(predictionCell, predictionInfo.status, predictionInfo.label);
      tr.appendChild(predictionCell);

      const evidenceCell = document.createElement("td");
      evidenceCell.className = "text-nowrap";
      const evidenceInfo = evidenceAvailability(row, annotationStage);
      setStatusIndicator(evidenceCell, evidenceInfo.status, evidenceInfo.label);
      tr.appendChild(evidenceCell);

      const completenessCell = document.createElement("td");
      completenessCell.className = "text-nowrap";
      const completenessStatus = variantCompleteness(row);
      setStatusIndicator(
        completenessCell,
        completenessStatus,
        statusLabel(completenessStatus) || completenessStatus,
      );
      tr.appendChild(completenessCell);

      wireVariantRowSelection(row, tr);
      summaryBodyEl.appendChild(tr);
    }
    summaryTableEl.hidden = false;
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
      variantCell.textContent = row?.variant_key ?? "\u2014";
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

  function groupPredictionRows(rows) {
    const grouped = new Map();
    for (const row of rows || []) {
      const key = `${row?.variant_id || ""}|${row?.variant_key || ""}`;
      if (!grouped.has(key)) {
        grouped.set(key, {
          variant_id: row?.variant_id || null,
          variant_key: row?.variant_key || "\u2014",
          outputs: {},
        });
      }
      const entry = grouped.get(key);
      const predictorKey = row?.predictor_key || "";
      if (predictorKey) {
        entry.outputs[predictorKey] = row;
      }
    }
    return Array.from(grouped.values());
  }

  function renderPredictorCell(td, output) {
    if (!output) {
      td.textContent = "\u2014";
      return;
    }
    td.className = "small";

    const outcome = document.createElement("div");
    outcome.className = "fw-semibold text-nowrap";
    outcome.appendChild(
      buildStatusIndicator(output?.outcome, statusLabel(output?.outcome) || output?.outcome || "\u2014"),
    );
    td.appendChild(outcome);

    if (output?.score != null) {
      const score = document.createElement("div");
      score.className = "text-secondary";
      score.textContent = `score: ${output.score}`;
      td.appendChild(score);
    }
    if (output?.label != null && output?.label !== "") {
      const label = document.createElement("div");
      label.className = "text-secondary";
      label.textContent = `label: ${output.label}`;
      td.appendChild(label);
    }
    if (output?.outcome !== "computed") {
      const reason = output?.reason_message || output?.reason_code || "";
      if (reason) {
        const reasonEl = document.createElement("div");
        reasonEl.className = "text-secondary";
        reasonEl.textContent = reason;
        td.appendChild(reasonEl);
      }
    }
  }

  function renderPredictionRows(variantRows) {
    clearEl(predBodyEl);
    for (const row of variantRows) {
      const tr = document.createElement("tr");

      const variantCell = document.createElement("td");
      variantCell.className = "text-nowrap";
      variantCell.textContent = row?.variant_key ?? "\u2014";
      tr.appendChild(variantCell);

      const siftCell = document.createElement("td");
      renderPredictorCell(siftCell, row?.outputs?.sift || null);
      tr.appendChild(siftCell);

      const polyphenCell = document.createElement("td");
      renderPredictorCell(polyphenCell, row?.outputs?.polyphen2 || null);
      tr.appendChild(polyphenCell);

      const alphaCell = document.createElement("td");
      renderPredictorCell(alphaCell, row?.outputs?.alphamissense || null);
      tr.appendChild(alphaCell);

      const notesCell = document.createElement("td");
      const notes = [];
      for (const key of ["sift", "polyphen2", "alphamissense"]) {
        const output = row?.outputs?.[key];
        if (!output) continue;
        const reason = output?.reason_message || output?.reason_code;
        if (!reason) continue;
        notes.push(`${predictorLabel(key)}: ${reason}`);
      }
      notesCell.textContent = notes.length > 0 ? notes.join(" | ") : "\u2014";
      tr.appendChild(notesCell);

      wireVariantRowSelection(row, tr);
      predBodyEl.appendChild(tr);
    }
    predTableEl.hidden = false;
  }

  function predictionRowsForDisplay(rows) {
    const source = groupPredictionRows(Array.isArray(rows) ? rows : []);
    if (predShowNotApplicableEl?.checked) {
      return source;
    }
    return source.filter((row) => {
      const outputs = row?.outputs || {};
      const predictors = ["sift", "polyphen2", "alphamissense"];
      for (const key of predictors) {
        const out = outputs[key];
        if (!out) return true;
        if (String(out?.outcome || "") !== "not_applicable") return true;
      }
      return false;
    });
  }

  function renderDbsnpEvidenceRows(rows, bodyEl, tableEl, mode) {
    if (!tableEl || !bodyEl) return;
    clearEl(bodyEl);
    for (const row of rows) {
      const tr = document.createElement("tr");

      const variantCell = document.createElement("td");
      variantCell.className = "text-nowrap";
      variantCell.textContent = row?.variant_key ?? "\u2014";
      tr.appendChild(variantCell);

      const outcomeCell = document.createElement("td");
      outcomeCell.className = "text-nowrap";
      outcomeCell.appendChild(
        buildStatusIndicator(row?.outcome, statusLabel(row?.outcome) || row?.outcome || "\u2014"),
      );
      tr.appendChild(outcomeCell);

      const rsidCell = document.createElement("td");
      rsidCell.className = "text-nowrap";
      rsidCell.textContent = row?.rsid ?? "\u2014";
      tr.appendChild(rsidCell);

      if (mode === "full") {
        const reasonCell = document.createElement("td");
        reasonCell.textContent = row?.reason_message || row?.reason_code || "\u2014";
        tr.appendChild(reasonCell);

        const retrievedCell = document.createElement("td");
        retrievedCell.className = "text-nowrap";
        retrievedCell.textContent = row?.retrieved_at ? formatDateTime(row.retrieved_at) : "\u2014";
        tr.appendChild(retrievedCell);
      }

      bodyEl.appendChild(tr);
    }
    tableEl.hidden = false;
  }

  function renderClinvarEvidenceRows(rows, bodyEl, tableEl, mode) {
    if (!tableEl || !bodyEl) return;
    clearEl(bodyEl);
    for (const row of rows) {
      const tr = document.createElement("tr");

      const variantCell = document.createElement("td");
      variantCell.className = "text-nowrap";
      variantCell.textContent = row?.variant_key ?? "\u2014";
      tr.appendChild(variantCell);

      const outcomeCell = document.createElement("td");
      outcomeCell.className = "text-nowrap";
      outcomeCell.appendChild(
        buildStatusIndicator(row?.outcome, statusLabel(row?.outcome) || row?.outcome || "\u2014"),
      );
      tr.appendChild(outcomeCell);

      const idCell = document.createElement("td");
      idCell.className = "text-nowrap";
      idCell.textContent = row?.clinvar_id ?? "\u2014";
      tr.appendChild(idCell);

      const sigCell = document.createElement("td");
      sigCell.textContent = row?.clinical_significance ?? "\u2014";
      tr.appendChild(sigCell);

      if (mode === "full") {
        const reasonCell = document.createElement("td");
        reasonCell.textContent = row?.reason_message || row?.reason_code || "\u2014";
        tr.appendChild(reasonCell);

        const retrievedCell = document.createElement("td");
        retrievedCell.className = "text-nowrap";
        retrievedCell.textContent = row?.retrieved_at ? formatDateTime(row.retrieved_at) : "\u2014";
        tr.appendChild(retrievedCell);
      }

      bodyEl.appendChild(tr);
    }
    tableEl.hidden = false;
  }

  function renderGnomadEvidenceRows(rows, bodyEl, tableEl, mode) {
    if (!tableEl || !bodyEl) return;
    clearEl(bodyEl);
    for (const row of rows) {
      const tr = document.createElement("tr");

      const variantCell = document.createElement("td");
      variantCell.className = "text-nowrap";
      variantCell.textContent = row?.variant_key ?? "\u2014";
      tr.appendChild(variantCell);

      const outcomeCell = document.createElement("td");
      outcomeCell.className = "text-nowrap";
      outcomeCell.appendChild(
        buildStatusIndicator(row?.outcome, statusLabel(row?.outcome) || row?.outcome || "\u2014"),
      );
      tr.appendChild(outcomeCell);

      const idCell = document.createElement("td");
      idCell.className = "text-nowrap";
      idCell.textContent = row?.gnomad_variant_id ?? "\u2014";
      tr.appendChild(idCell);

      const afCell = document.createElement("td");
      afCell.className = "text-nowrap";
      afCell.textContent = row?.global_af != null ? String(row.global_af) : "\u2014";
      tr.appendChild(afCell);

      if (mode === "full") {
        const reasonCell = document.createElement("td");
        reasonCell.textContent = row?.reason_message || row?.reason_code || "\u2014";
        tr.appendChild(reasonCell);

        const retrievedCell = document.createElement("td");
        retrievedCell.className = "text-nowrap";
        retrievedCell.textContent = row?.retrieved_at ? formatDateTime(row.retrieved_at) : "\u2014";
        tr.appendChild(retrievedCell);
      }

      bodyEl.appendChild(tr);
    }
    tableEl.hidden = false;
  }

  function renderEvidenceDiagnosticsTable(rows) {
    clearEl(annotationDiagnosticsBodyEl);
    for (const row of rows) {
      const tr = document.createElement("tr");

      const sourceTd = document.createElement("td");
      sourceTd.className = "text-nowrap";
      sourceTd.textContent = row.source;
      tr.appendChild(sourceTd);

      const enabledTd = document.createElement("td");
      enabledTd.textContent = row.enabled;
      tr.appendChild(enabledTd);

      const outcomeTd = document.createElement("td");
      outcomeTd.appendChild(
        buildStatusIndicator(row.outcome, statusLabel(row.outcome) || row.outcome || "\u2014"),
      );
      tr.appendChild(outcomeTd);

      const completenessTd = document.createElement("td");
      completenessTd.className = "text-nowrap";
      const completenessKey = row.completenessKey || "";
      const completenessLabel = row.completenessImpact || "\u2014";
      completenessTd.appendChild(buildStatusIndicator(completenessKey, completenessLabel));
      tr.appendChild(completenessTd);

      const errorsTd = document.createElement("td");
      errorsTd.textContent = String(row.errors);
      tr.appendChild(errorsTd);

      const retryTd = document.createElement("td");
      retryTd.textContent = Number.isFinite(row.retryAttempts) ? String(row.retryAttempts) : "\u2014";
      tr.appendChild(retryTd);

      const errorSampleTd = document.createElement("td");
      errorSampleTd.textContent = row.errorSample || "\u2014";
      tr.appendChild(errorSampleTd);

      const reasonsTd = document.createElement("td");
      reasonsTd.textContent = row.topReasons || "\u2014";
      tr.appendChild(reasonsTd);

      const httpStatusesTd = document.createElement("td");
      httpStatusesTd.textContent = row.httpStatuses || "\u2014";
      tr.appendChild(httpStatusesTd);

      const hintTd = document.createElement("td");
      hintTd.textContent = row.hint || "\u2014";
      tr.appendChild(hintTd);

      annotationDiagnosticsBodyEl.appendChild(tr);
    }
    annotationDiagnosticsTableEl.hidden = rows.length === 0;
  }

  function renderAnnotation(stage) {
    const failed = stageFailureText(stage, "Annotation failed.");
    if (failed) {
      setMessage(annotationMessageEl, failed);
      const details = stage?.error?.details && typeof stage.error.details === "object"
        ? stage.error.details
        : {};
      const strictNoSource = stage?.error?.code === "EVIDENCE_SOURCES_UNAVAILABLE";
      const missingSources = Array.isArray(details?.missing_sources)
        ? details.missing_sources.filter((value) => typeof value === "string" && value.trim())
        : [];
      const missingOutputs = Array.isArray(details?.missing_outputs)
        ? details.missing_outputs.filter((value) => typeof value === "string" && value.trim())
        : [];
      const blockedOutputs = Array.isArray(details?.blocked_outputs)
        ? details.blocked_outputs.filter((value) => typeof value === "string" && value.trim())
        : [];
      const policyText = formatEvidencePolicy(
        details?.annotation_evidence_policy || stage?.stats?.annotation_evidence_policy || "",
      );
      const requestedMode = details?.requested_mode || stage?.stats?.evidence_mode_requested || "";
      const effectiveMode = details?.effective_mode || stage?.stats?.evidence_mode_effective || "";
      const failedSource = details?.failed_source || "";
      setSummaryRows(annotationSummaryEl, [
        { label: "Evidence completeness", value: formatCompleteness(details?.annotation_evidence_completeness) },
        { label: "Evidence policy", value: policyText || "" },
        { label: "Failed source", value: failedSource },
        { label: "Evidence mode requested", value: requestedMode },
        { label: "Evidence mode effective", value: effectiveMode },
        { label: "Missing sources", value: missingSources.join(", ") },
        { label: "Blocked outputs", value: blockedOutputs.join(", ") },
        { label: "Missing outputs", value: missingOutputs.join(", ") },
      ]);
      const strictMissingText = missingSources.length > 0
        ? `Missing evidence sources: ${missingSources.join(", ")}.`
        : "";
      setMessage(
        annotationDiagnosticsMessageEl,
        details?.hint
          || (strictNoSource ? strictMissingText : "")
          || "Evidence retrieval failed. Retry with a different policy or fix source connectivity.",
      );
      const stageStats = stage?.stats && typeof stage.stats === "object" ? stage.stats : {};
      const sourceCompletenessRaw =
        details?.evidence_source_completeness && typeof details.evidence_source_completeness === "object"
          ? details.evidence_source_completeness
          : (
            stageStats?.evidence_source_completeness
            && typeof stageStats.evidence_source_completeness === "object"
              ? stageStats.evidence_source_completeness
              : {}
          );
      const sourceCompletenessReasonRaw =
        details?.evidence_source_completeness_reason
        && typeof details.evidence_source_completeness_reason === "object"
          ? details.evidence_source_completeness_reason
          : (
            stageStats?.evidence_source_completeness_reason
            && typeof stageStats.evidence_source_completeness_reason === "object"
              ? stageStats.evidence_source_completeness_reason
              : {}
          );
      const knownSources = new Set(["dbsnp", "clinvar", "gnomad"]);
      const failedSourceKey = knownSources.has(failedSource) ? failedSource : "";
      const hasEvidenceDiagnostics =
        Object.keys(sourceCompletenessRaw).length > 0
        || Object.keys(sourceCompletenessReasonRaw).length > 0
        || Boolean(failedSourceKey)
        || Boolean(stageStats?.dbsnp_errors || stageStats?.clinvar_errors || stageStats?.gnomad_errors);

      if (hasEvidenceDiagnostics) {
        const diagnosticStats = { ...stageStats };
        if (failedSourceKey) {
          if (!diagnosticStats[`${failedSourceKey}_error_details`]) {
            diagnosticStats[`${failedSourceKey}_error_details`] = details;
          }
          if (diagnosticStats[`${failedSourceKey}_errors`] == null) {
            const errorCount = Number(details?.error_count ?? 0);
            diagnosticStats[`${failedSourceKey}_errors`] = Number.isFinite(errorCount)
              ? errorCount
              : Array.isArray(details?.errors)
                ? details.errors.length
                : 0;
          }
          if (diagnosticStats[`${failedSourceKey}_retry_attempts`] == null) {
            const retries = Number(details?.retry_attempts_total ?? 0);
            diagnosticStats[`${failedSourceKey}_retry_attempts`] = Number.isFinite(retries) ? retries : 0;
          }
        }

        const diagnosticsRows = buildEvidenceDiagnosticsRows(
          diagnosticStats,
          sourceCompletenessRaw,
          sourceCompletenessReasonRaw,
        );
        renderEvidenceDiagnosticsTable(diagnosticsRows);
      } else {
        hideAnnotationDiagnosticsTable();
      }
      setMessage(annotationVcfMessageEl, "");
      hideAnnotationVcfTable();
      return;
    }

    if (!stage || stage.status !== "succeeded") {
      setMessage(annotationMessageEl, stageStatusText(stage?.status, "Waiting for annotation."));
      setSummaryRows(annotationSummaryEl, []);
      setMessage(annotationDiagnosticsMessageEl, stageStatusText(stage?.status, "Waiting for evidence diagnostics."));
      hideAnnotationDiagnosticsTable();
      setMessage(annotationVcfMessageEl, stageStatusText(stage?.status, "Waiting for annotated VCF preview."));
      hideAnnotationVcfTable();
      return;
    }

    const stats = stage?.stats ?? {};
    const overallCompleteness = normalizeCompleteness(stats?.annotation_evidence_completeness);
    const sourceCompletenessRaw =
      stats?.evidence_source_completeness && typeof stats.evidence_source_completeness === "object"
        ? stats.evidence_source_completeness
        : {};
    const sourceCompletenessReasonRaw =
      stats?.evidence_source_completeness_reason && typeof stats.evidence_source_completeness_reason === "object"
        ? stats.evidence_source_completeness_reason
        : {};
    const evidenceSummary = (prefix, label) => {
      if (stats?.[`${prefix}_enabled`] === false) {
        return stats?.[`${prefix}_note`] || `${label} disabled`;
      }
      const found = Number(stats?.[`${prefix}_found`] ?? 0);
      const notFound = Number(stats?.[`${prefix}_not_found`] ?? 0);
      const errors = Number(stats?.[`${prefix}_errors`] ?? 0);
      const skipped = Number(stats?.[`${prefix}_skipped_out_of_scope`] ?? 0);
      if (Number.isFinite(skipped) && skipped > 0) {
        return `${found} found / ${notFound} not found / ${errors} errors / ${skipped} skipped`;
      }
      return `${found} found / ${notFound} not found / ${errors} errors`;
    };
    const warnings = [];
    if (stats?.dbsnp_warning) warnings.push(String(stats.dbsnp_warning));
    if (stats?.clinvar_warning) warnings.push(String(stats.clinvar_warning));
    if (stats?.gnomad_warning) warnings.push(String(stats.gnomad_warning));
    setMessage(annotationMessageEl, "Annotation technical output is ready.");
    setSummaryRows(annotationSummaryEl, [
      { label: "Tool", value: stats?.tool },
      { label: "Genome", value: stats?.genome },
      { label: "Evidence completeness", value: formatCompleteness(overallCompleteness) },
      { label: "Evidence policy", value: formatEvidencePolicy(stats?.annotation_evidence_policy) },
      { label: "Configured", value: stats?.configured },
      { label: "Variants written", value: stats?.variants_written },
      { label: "Output VCF", value: basename(stats?.output_vcf_path) || stats?.output_vcf_path },
      { label: "Exit code", value: stats?.snpeff_exit_code },
      { label: "dbSNP", value: evidenceSummary("dbsnp", "dbSNP") },
      { label: "dbSNP completeness", value: formatCompleteness(sourceCompletenessRaw?.dbsnp) },
      { label: "ClinVar", value: evidenceSummary("clinvar", "ClinVar") },
      { label: "ClinVar completeness", value: formatCompleteness(sourceCompletenessRaw?.clinvar) },
      { label: "gnomAD", value: evidenceSummary("gnomad", "gnomAD") },
      { label: "gnomAD completeness", value: formatCompleteness(sourceCompletenessRaw?.gnomad) },
      { label: "Evidence warnings", value: warnings.length > 0 ? warnings.join(" | ") : "" },
      { label: "Note", value: stats?.note },
    ]);

    const diagnosticsRows = buildEvidenceDiagnosticsRows(
      stats,
      sourceCompletenessRaw,
      sourceCompletenessReasonRaw,
    );
    renderEvidenceDiagnosticsTable(diagnosticsRows);
    const hasAnyEvidenceErrors = diagnosticsRows.some((row) => row.errors > 0);
    let diagnosticsMessage = "No evidence retrieval errors detected.";
    if (overallCompleteness === "partial") {
      diagnosticsMessage = "Evidence retrieval completed with partial coverage.";
    } else if (overallCompleteness === "unavailable") {
      diagnosticsMessage = "Evidence retrieval is unavailable for one or more sources.";
    }
    if (hasAnyEvidenceErrors) {
      diagnosticsMessage = "Evidence retrieval encountered one or more errors.";
    }
    setMessage(
      annotationDiagnosticsMessageEl,
      diagnosticsMessage,
    );
  }

  async function refreshAnnotationOutput(runId, stage) {
    const failed = stageFailureText(stage, "Annotation failed.");
    if (failed) {
      setMessage(annotationVcfMessageEl, "");
      hideAnnotationVcfTable();
      updateSimplePager(0, false, annotationVcfPagePrevEl, annotationVcfPageNextEl, annotationVcfPageLabelEl, "");
      annotationVcfHasNext = false;
      return;
    }
    if (!stage || stage.status !== "succeeded") {
      setMessage(annotationVcfMessageEl, stageStatusText(stage?.status, "Waiting for annotated VCF preview."));
      hideAnnotationVcfTable();
      updateSimplePager(0, false, annotationVcfPagePrevEl, annotationVcfPageNextEl, annotationVcfPageLabelEl, "");
      annotationVcfHasNext = false;
      return;
    }

    try {
      const { resp, payload } = await getJson(
        buildAnnotationVcfPreviewUrl(runId, annotationVcfPage, annotationVcfPosFilter),
      );
      if (!resp.ok || !payload?.ok) {
        setMessage(
          annotationVcfMessageEl,
          payload?.error?.message || "Unable to load annotated VCF preview.",
        );
        hideAnnotationVcfTable();
        updateSimplePager(0, false, annotationVcfPagePrevEl, annotationVcfPageNextEl, annotationVcfPageLabelEl, "");
        annotationVcfHasNext = false;
        return;
      }

      const lines = Array.isArray(payload?.data?.preview_lines) ? payload.data.preview_lines : [];
      const truncated = Boolean(payload?.data?.truncated);
      if (lines.length === 0) {
        const msg = annotationVcfPosFilter
          ? formatVcfPreviewMessage(annotationVcfPosFilter, 0, false)
          : "Annotation stage succeeded but annotated VCF content is not available for preview.";
        setMessage(annotationVcfMessageEl, msg);
        hideAnnotationVcfTable();
        updateSimplePager(0, false, annotationVcfPagePrevEl, annotationVcfPageNextEl, annotationVcfPageLabelEl, "");
        annotationVcfHasNext = false;
        return;
      }

      const parsed = parseVcfPreviewLines(lines);
      if (!parsed) {
        setMessage(annotationVcfMessageEl, "Annotated VCF preview is available but could not be parsed as tabular VCF.");
        hideAnnotationVcfTable();
        updateSimplePager(0, false, annotationVcfPagePrevEl, annotationVcfPageNextEl, annotationVcfPageLabelEl, "");
        annotationVcfHasNext = false;
        return;
      }

      const rowCount = Number(payload?.data?.data_line_count ?? parsed.dataRows.length ?? 0);
      annotationVcfHasNext = !annotationVcfPosFilter && truncated;
      const note = annotationVcfPosFilter ? `search: ${rowCount} matches` : `${rowCount} rows`;
      updateSimplePager(
        annotationVcfPage,
        annotationVcfHasNext,
        annotationVcfPagePrevEl,
        annotationVcfPageNextEl,
        annotationVcfPageLabelEl,
        note,
      );
      setMessage(annotationVcfMessageEl, formatVcfPreviewMessage(annotationVcfPosFilter, rowCount, truncated));
      renderAnnotationVcfTable(parsed);
    } catch {
      setMessage(annotationVcfMessageEl, "Unable to load annotated VCF preview.");
      hideAnnotationVcfTable();
      updateSimplePager(0, false, annotationVcfPagePrevEl, annotationVcfPageNextEl, annotationVcfPageLabelEl, "");
      annotationVcfHasNext = false;
    }
  }

  function shouldRefresh(lastAt, intervalMs) {
    if (!intervalMs || intervalMs <= 0) return true;
    return Date.now() - lastAt >= intervalMs;
  }

  function artifactReasonMessage(artifact, label, stageLabel) {
    if (!artifact) return `${label} artifact is not listed.`;
    if (artifact.available) return "";
    if (artifact.reason === "stage_not_ready") {
      return `Waiting for ${stageLabel} stage to finish.`;
    }
    if (artifact.reason === "not_found") return `${label} artifact not found.`;
    return `${label} artifact is not available.`;
  }

  function htmlArtifactsMissingMessage(htmlArtifacts) {
    const reportingStage = stageByName(latestStages, "reporting");
    if (!reportingStage || reportingStage.status !== "succeeded") {
      return "HTML summaries are not available yet (reporting stage not complete).";
    }
    if (!Array.isArray(htmlArtifacts) || htmlArtifacts.length === 0) {
      return "HTML summaries not retained or not available for this run. Reason: no HTML artifacts listed.";
    }
    const reasons = htmlArtifacts
      .map((item) => artifactReasonMessage(item, item.name, item?.stage || "reporting"))
      .filter((reason) => reason);
    const uniqueReasons = Array.from(new Set(reasons));
    const reasonText = uniqueReasons.length > 0 ? ` Reason: ${uniqueReasons.join(" | ")}` : "";
    return `HTML summaries not retained or not available for this run.${reasonText}`;
  }

  function updateArtifactsSectionMessage(artifacts, stageName, messageEl) {
    if (!messageEl) return;
    const items = artifacts.filter((item) => item.stage === stageName && item.kind !== "html");
    if (items.length === 0) {
      setMessage(messageEl, "Artifacts are not listed for this stage yet.");
      return;
    }
    if (items.some((item) => item.available)) {
      setMessage(messageEl, "");
      return;
    }
    if (items.some((item) => item.reason === "stage_not_ready")) {
      setMessage(messageEl, `Waiting for ${stageName} stage artifacts.`);
      return;
    }
    setMessage(messageEl, "Artifacts not found for this stage.");
  }

  function renderHtmlArtifactCard(containerEl, name, htmlText, truncated) {
    if (!containerEl) return;
    const card = document.createElement("div");
    card.className = "card shadow-sm mb-3";

    const header = document.createElement("div");
    header.className = "card-header small fw-semibold";
    header.textContent = name;
    card.appendChild(header);

    const body = document.createElement("div");
    body.className = "card-body p-0";
    const iframe = document.createElement("iframe");
    iframe.className = "w-100 border-0";
    iframe.setAttribute("sandbox", "allow-same-origin allow-scripts");
    iframe.setAttribute("title", name || "HTML artifact preview");
    iframe.setAttribute("tabindex", "0");
    iframe.style.minHeight = "600px";
    iframe.srcdoc = htmlText || "";
    body.appendChild(iframe);

    if (truncated) {
      const notice = document.createElement("div");
      notice.className = "small text-secondary px-3 py-2 border-top";
      notice.textContent = "Preview truncated for this HTML artifact.";
      body.appendChild(notice);
    }

    card.appendChild(body);
    containerEl.appendChild(card);
  }

  async function refreshAnnotationEvidence(runId, stage) {
    const failed = stageFailureText(stage, "Annotation failed.");
    if (failed) {
      setMessage(annotationDbsnpMessageEl, failed);
      hideTable(annotationDbsnpTableEl, annotationDbsnpBodyEl);
      setMessage(annotationClinvarMessageEl, failed);
      hideTable(annotationClinvarTableEl, annotationClinvarBodyEl);
      setMessage(annotationGnomadMessageEl, failed);
      hideTable(annotationGnomadTableEl, annotationGnomadBodyEl);
      return;
    }

    if (!stage || stage.status !== "succeeded") {
      const waitMessage = stageStatusText(stage?.status, "Waiting for annotation.");
      setMessage(annotationDbsnpMessageEl, waitMessage);
      hideTable(annotationDbsnpTableEl, annotationDbsnpBodyEl);
      setMessage(annotationClinvarMessageEl, waitMessage);
      hideTable(annotationClinvarTableEl, annotationClinvarBodyEl);
      setMessage(annotationGnomadMessageEl, waitMessage);
      hideTable(annotationGnomadTableEl, annotationGnomadBodyEl);
      return;
    }

    if (!shouldRefresh(lastEvidenceFetchedAt, EVIDENCE_REFRESH_MS)) return;
    lastEvidenceFetchedAt = Date.now();

    try {
      const classification = annotationEvidenceFilterEl
        ? String(annotationEvidenceFilterEl.value || "").trim()
        : "";
      const outcome = annotationEvidenceOutcomeEl
        ? String(annotationEvidenceOutcomeEl.value || "").trim()
        : "";
      const classificationParam = classification
        ? `&classification=${encodeURIComponent(classification)}`
        : "";
      const outcomeParam = outcome ? `&outcome=${encodeURIComponent(outcome)}` : "";
      const [dbsnpResult, clinvarResult, gnomadResult] = await Promise.all([
        getJson(
          `/api/v1/runs/${encodeURIComponent(runId)}/dbsnp_evidence?limit=${EVIDENCE_LIMIT}${classificationParam}${outcomeParam}`,
        ),
        getJson(
          `/api/v1/runs/${encodeURIComponent(runId)}/clinvar_evidence?limit=${EVIDENCE_LIMIT}${classificationParam}${outcomeParam}`,
        ),
        getJson(
          `/api/v1/runs/${encodeURIComponent(runId)}/gnomad_evidence?limit=${EVIDENCE_LIMIT}${classificationParam}${outcomeParam}`,
        ),
      ]);

      const evidenceResults = [
        {
          key: "dbsnp_evidence",
          result: dbsnpResult,
          annotationMessageEl: annotationDbsnpMessageEl,
          annotationTableEl: annotationDbsnpTableEl,
          annotationBodyEl: annotationDbsnpBodyEl,
          render: (rows, bodyEl, tableEl, mode) => renderDbsnpEvidenceRows(rows, bodyEl, tableEl, mode),
        },
        {
          key: "clinvar_evidence",
          result: clinvarResult,
          annotationMessageEl: annotationClinvarMessageEl,
          annotationTableEl: annotationClinvarTableEl,
          annotationBodyEl: annotationClinvarBodyEl,
          render: (rows, bodyEl, tableEl, mode) => renderClinvarEvidenceRows(rows, bodyEl, tableEl, mode),
        },
        {
          key: "gnomad_evidence",
          result: gnomadResult,
          annotationMessageEl: annotationGnomadMessageEl,
          annotationTableEl: annotationGnomadTableEl,
          annotationBodyEl: annotationGnomadBodyEl,
          render: (rows, bodyEl, tableEl, mode) => renderGnomadEvidenceRows(rows, bodyEl, tableEl, mode),
        },
      ];

      for (const entry of evidenceResults) {
        const { result } = entry;
        if (result?.resp?.status === 404) {
          setMessage(entry.annotationMessageEl, "No run found.");
          hideTable(entry.annotationTableEl, entry.annotationBodyEl);
          continue;
        }
        if (!result?.resp?.ok || !result?.payload?.ok) {
          const errorMessage = result?.payload?.error?.message || "Unable to load evidence outputs.";
          setMessage(entry.annotationMessageEl, errorMessage);
          hideTable(entry.annotationTableEl, entry.annotationBodyEl);
          continue;
        }

        const rows = Array.isArray(result?.payload?.data?.[entry.key])
          ? result.payload.data[entry.key]
          : [];
        if (rows.length === 0) {
          const emptyMessage = "No evidence rows available for current upload.";
          setMessage(entry.annotationMessageEl, emptyMessage);
          hideTable(entry.annotationTableEl, entry.annotationBodyEl);
          continue;
        }

        entry.render(rows, entry.annotationBodyEl, entry.annotationTableEl, "full");
        setMessage(entry.annotationMessageEl, "");
      }
    } catch {
      const errMessage = "Unable to load evidence outputs.";
      setMessage(annotationDbsnpMessageEl, errMessage);
      hideTable(annotationDbsnpTableEl, annotationDbsnpBodyEl);
      setMessage(annotationClinvarMessageEl, errMessage);
      hideTable(annotationClinvarTableEl, annotationClinvarBodyEl);
      setMessage(annotationGnomadMessageEl, errMessage);
      hideTable(annotationGnomadTableEl, annotationGnomadBodyEl);
    }
  }

  async function refreshArtifacts(runId) {
    if (!shouldRefresh(lastArtifactsFetchedAt, ARTIFACT_REFRESH_MS)) return;
    lastArtifactsFetchedAt = Date.now();

    try {
      const { resp, payload } = await getJson(`/api/v1/runs/${encodeURIComponent(runId)}/artifacts`);
      if (!resp.ok || !payload?.ok) {
        const message = payload?.error?.message || "Unable to load artifact list.";
        setMessage(classificationArtifactsMessageEl, message);
        setMessage(predictionArtifactsMessageEl, message);
        setMessage(finalHtmlArtifactsMessageEl, message);
        return;
      }

      const artifacts = Array.isArray(payload?.data?.artifacts) ? payload.data.artifacts : [];
      const artifactsByName = new Map(artifacts.map((item) => [item.name, item]));

      updateArtifactsSectionMessage(artifacts, "classification", classificationArtifactsMessageEl);
      updateArtifactsSectionMessage(artifacts, "prediction", predictionArtifactsMessageEl);

      const previewTasks = [];

      function queueVcfPreview(name, messageEl, tableEl, headRowEl, bodyEl, options) {
        const artifact = artifactsByName.get(name);
        const message = artifactReasonMessage(artifact, name, artifact?.stage || "pipeline");
        if (!artifact || !artifact.available) {
          setMessage(messageEl, message);
          hideVcfTable(tableEl, headRowEl, bodyEl);
          return;
        }
        previewTasks.push(
          getJson(
            `/api/v1/runs/${encodeURIComponent(runId)}/artifacts/preview?name=${encodeURIComponent(name)}&limit=${ARTIFACT_PREVIEW_LIMIT}`,
          ).then(({ resp: previewResp, payload: previewPayload }) => {
            if (!previewResp.ok || !previewPayload?.ok) {
              setMessage(messageEl, previewPayload?.error?.message || "Unable to load artifact preview.");
              hideVcfTable(tableEl, headRowEl, bodyEl);
              return;
            }
            const artifactData = previewPayload?.data?.artifact;
            if (!artifactData?.available) {
              setMessage(messageEl, "Artifact preview not available.");
              hideVcfTable(tableEl, headRowEl, bodyEl);
              return;
            }
            const lines = Array.isArray(artifactData?.preview_lines) ? artifactData.preview_lines : [];
            if (lines.length === 0) {
              setMessage(messageEl, "Artifact preview is empty.");
              hideVcfTable(tableEl, headRowEl, bodyEl);
              return;
            }
            const parsed = parseVcfPreviewLines(lines);
            if (!parsed) {
              setMessage(messageEl, "Artifact preview could not be parsed as VCF.");
              hideVcfTable(tableEl, headRowEl, bodyEl);
              return;
            }
            renderVcfTable(parsed, tableEl, headRowEl, bodyEl, options || {});
            const rowCount = parsed.dataRows.length;
            if (artifactData?.truncated) {
              setMessage(messageEl, `Showing first ${rowCount} variant rows (preview truncated).`);
            } else {
              setMessage(messageEl, `Showing ${rowCount} variant rows.`);
            }
          }),
        );
      }

      function queueJsonlPreview(name, messageEl, tableEl, bodyEl, options) {
        const artifact = artifactsByName.get(name);
        const message = artifactReasonMessage(artifact, name, artifact?.stage || "pipeline");
        if (!artifact || !artifact.available) {
          setMessage(messageEl, message);
          hideJsonlTable(tableEl, bodyEl);
          return;
        }
        previewTasks.push(
          getJson(
            `/api/v1/runs/${encodeURIComponent(runId)}/artifacts/preview?name=${encodeURIComponent(name)}&limit=${ARTIFACT_PREVIEW_LIMIT}`,
          ).then(({ resp: previewResp, payload: previewPayload }) => {
            if (!previewResp.ok || !previewPayload?.ok) {
              setMessage(messageEl, previewPayload?.error?.message || "Unable to load artifact preview.");
              hideJsonlTable(tableEl, bodyEl);
              return;
            }
            const artifactData = previewPayload?.data?.artifact;
            if (!artifactData?.available) {
              setMessage(messageEl, "Artifact preview not available.");
              hideJsonlTable(tableEl, bodyEl);
              return;
            }
            const rows = Array.isArray(artifactData?.rows) ? artifactData.rows : [];
            if (rows.length === 0) {
              setMessage(messageEl, "Artifact preview is empty.");
              hideJsonlTable(tableEl, bodyEl);
              return;
            }
            if (options?.mode === "summary") {
              renderJsonlSummaryTable(rows, tableEl, bodyEl);
            } else {
              renderJsonlRecordTable(rows, tableEl, bodyEl);
            }
            if (artifactData?.truncated) {
              setMessage(messageEl, `Showing ${rows.length} records (preview truncated).`);
            } else {
              setMessage(messageEl, `Showing ${rows.length} records.`);
            }
          }),
        );
      }

      previewTasks.push(
        refreshArtifactVcfPreview({
          runId,
          artifactsByName,
          artifactName: "classification.input.vcf",
          messageEl: classificationInputVcfMessageEl,
          tableEl: classificationInputVcfTableEl,
          headRowEl: classificationInputVcfHeadRowEl,
          bodyEl: classificationInputVcfBodyEl,
          pageIndex: clsInputVcfPage,
          posFilter: clsInputVcfPosFilter,
          prevEl: classificationInputVcfPagePrevEl,
          nextEl: classificationInputVcfPageNextEl,
          labelEl: classificationInputVcfPageLabelEl,
          setHasNext: (value) => {
            clsInputVcfHasNext = value;
          },
        }),
      );
      previewTasks.push(
        refreshArtifactVcfPreview({
          runId,
          artifactsByName,
          artifactName: "prediction.input.vcf",
          messageEl: predictionInputVcfMessageEl,
          tableEl: predictionInputVcfTableEl,
          headRowEl: predictionInputVcfHeadRowEl,
          bodyEl: predictionInputVcfBodyEl,
          pageIndex: predInputVcfPage,
          posFilter: predInputVcfPosFilter,
          prevEl: predictionInputVcfPagePrevEl,
          nextEl: predictionInputVcfPageNextEl,
          labelEl: predictionInputVcfPageLabelEl,
          setHasNext: (value) => {
            predInputVcfHasNext = value;
          },
        }),
      );

      const htmlArtifacts = artifacts.filter((item) => item.kind === "html");
      clearEl(finalHtmlArtifactsEl);
      const availableHtml = htmlArtifacts.filter((item) => item.available);
      htmlArtifactsReady = availableHtml.length > 0;
      if (availableHtml.length === 0) {
        setMessage(finalHtmlArtifactsMessageEl, htmlArtifactsMissingMessage(htmlArtifacts));
      } else {
        setMessage(finalHtmlArtifactsMessageEl, "");
        for (const item of availableHtml) {
          previewTasks.push(
            getJson(
              `/api/v1/runs/${encodeURIComponent(runId)}/artifacts/preview?name=${encodeURIComponent(item.name)}`,
            ).then(({ resp: previewResp, payload: previewPayload }) => {
              if (!previewResp.ok || !previewPayload?.ok) {
                setMessage(finalHtmlArtifactsMessageEl, "Unable to load HTML summaries.");
                return;
              }
              const artifactData = previewPayload?.data?.artifact;
              if (!artifactData?.available) {
                return;
              }
              renderHtmlArtifactCard(finalHtmlArtifactsEl, item.name, artifactData.html, artifactData.truncated);
            }),
          );
        }
      }

      if (previewTasks.length > 0) {
        await Promise.all(previewTasks);
      }
    } catch {
      htmlArtifactsReady = false;
      const message = "Unable to load artifact previews.";
      setMessage(classificationArtifactsMessageEl, message);
      setMessage(predictionArtifactsMessageEl, message);
      setMessage(finalHtmlArtifactsMessageEl, message);
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
    const completeness = normalizeCompleteness(stats?.annotation_evidence_completeness);
    if (completeness === "partial") {
      setMessage(reportingMessageEl, "Reporting synthesis is ready with partial evidence.");
    } else if (completeness === "unavailable") {
      setMessage(reportingMessageEl, "Reporting synthesis is ready, but one or more evidence sources are unavailable.");
    } else {
      setMessage(reportingMessageEl, "Reporting synthesis is ready.");
    }

    const completeSources = Array.isArray(stats?.evidence_complete_sources)
      ? stats.evidence_complete_sources.filter((value) => typeof value === "string" && value.trim())
      : [];
    const partialSources = Array.isArray(stats?.evidence_partial_sources)
      ? stats.evidence_partial_sources.filter((value) => typeof value === "string" && value.trim())
      : [];
    const unavailableSources = Array.isArray(stats?.evidence_unavailable_sources)
      ? stats.evidence_unavailable_sources.filter((value) => typeof value === "string" && value.trim())
      : [];
    const failedSources = Array.isArray(stats?.evidence_failed_sources)
      ? stats.evidence_failed_sources.filter((value) => typeof value === "string" && value.trim())
      : [];

    const coverageSummary = [
      `complete: ${completeSources.length}`,
      `partial: ${partialSources.length}`,
      `unavailable: ${unavailableSources.length}`,
    ].join(" | ");

    const rows = [];
    rows.push({ label: "Status", value: "Reporting stage succeeded." });
    rows.push({ label: "Evidence policy", value: formatEvidencePolicy(stats?.annotation_evidence_policy) });
    rows.push({ label: "Overall evidence completeness", value: formatCompleteness(completeness) });
    rows.push({ label: "Source coverage summary", value: coverageSummary });
    rows.push({ label: "Sources requiring attention", value: failedSources.join(", ") });
    rows.push({ label: "Details", value: "See Annotation for per-source diagnostics and error breakdowns." });
    setSummaryRows(reportingSummaryEl, rows);

    const significantRows = [];
    significantRows.push({
      label: "Parsing",
      value: buildStageSummary("parser", (parserStats) => {
        const persisted = formatSummaryValue(parserStats?.snv_records_persisted);
        const seen = formatSummaryValue(parserStats?.records_seen);
        const nonSnv = formatSummaryValue(parserStats?.non_snv_alleles_skipped);
        const duplicates = formatSummaryValue(parserStats?.duplicate_records_ignored);
        return `SNVs persisted: ${persisted} | Records seen: ${seen} | Non-SNV skipped: ${nonSnv} | Duplicates ignored: ${duplicates}`;
      }),
    });
    significantRows.push({
      label: "Pre-annotation",
      value: buildStageSummary("pre_annotation", (preStats) => {
        const processed = formatSummaryValue(preStats?.variants_processed);
        return `Variants processed: ${processed}`;
      }),
    });
    significantRows.push({
      label: "Classification",
      value: buildStageSummary("classification", (clsStats) => {
        const processed = formatSummaryValue(clsStats?.variants_processed);
        const unclassified = formatSummaryValue(clsStats?.unclassified_count);
        const categoryCounts = formatCategoryCounts(clsStats?.category_counts);
        return `Variants processed: ${processed} | Unclassified: ${unclassified} | Category counts: ${categoryCounts}`;
      }),
    });
    significantRows.push({
      label: "Prediction",
      value: buildStageSummary("prediction", (predStats) => {
        const missense = formatSummaryValue(predStats?.missense_variants);
        const siftPoly = formatSummaryValue(predStats?.missense_with_sift_or_polyphen);
        const alphamissense = formatSummaryValue(predStats?.missense_with_alphamissense);
        return `Missense variants: ${missense} | SIFT/PolyPhen2: ${siftPoly} | AlphaMissense: ${alphamissense}`;
      }),
    });
    significantRows.push({
      label: "Annotation",
      value: buildStageSummary("annotation", (annStats) => {
        const dbsnp = formatSummaryValue(annStats?.dbsnp_found);
        const clinvar = formatSummaryValue(annStats?.clinvar_found);
        const gnomad = formatSummaryValue(annStats?.gnomad_found);
        const annComplete =
          formatCompleteness(annStats?.annotation_evidence_completeness) || "\u2014";
        return `dbSNP found: ${dbsnp} | ClinVar found: ${clinvar} | gnomAD found: ${gnomad} | Evidence completeness: ${annComplete}`;
      }),
    });
    significantRows.push({
      label: "Reporting",
      value: buildStageSummary("reporting", () => {
        const overall = formatCompleteness(completeness) || "\u2014";
        return `Evidence completeness: ${overall}`;
      }),
    });
    setSummaryRows(reportingSignificantEl, significantRows);

    const annotationStage = stageByName(latestStages, "annotation");
    const annotationStats = annotationStage?.stats ?? {};
    const annotationCompleteness =
      annotationStats?.evidence_source_completeness && typeof annotationStats.evidence_source_completeness === "object"
        ? annotationStats.evidence_source_completeness
        : {};
    const annotationCompletenessReason =
      annotationStats?.evidence_source_completeness_reason
      && typeof annotationStats.evidence_source_completeness_reason === "object"
        ? annotationStats.evidence_source_completeness_reason
        : {};
    const reportingDiagnostics = buildEvidenceDiagnosticsRows(
      annotationStats,
      annotationCompleteness,
      annotationCompletenessReason,
    );
    const diagnosticsSummaryRows = reportingDiagnostics.map((row) => {
      const retries = Number.isFinite(row.retryAttempts) ? String(row.retryAttempts) : "\u2014";
      return {
        label: `${row.source} diagnostics`,
        value: `Outcome: ${row.outcome} | Error: ${row.errorSample} | Retries: ${retries} | Completeness: ${row.completenessImpact}`,
      };
    });
    setSummaryRows(reportingEvidenceDiagnosticsEl, diagnosticsSummaryRows);
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
      setMessage(finalMessageEl, "Pipeline complete. Final output is available.");
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
      updatePager(preTotalCount, PRE_PAGE_SIZE, prePage, prePagePrevEl, prePageNextEl, prePageLabelEl);
      return;
    }
    if (!stage || stage.status !== "succeeded") {
      hideTable(preTableEl, preBodyEl);
      setMessage(preMessageEl, stageStatusText(stage?.status, "Waiting for pre-annotation."));
      updatePager(preTotalCount, PRE_PAGE_SIZE, prePage, prePagePrevEl, prePageNextEl, prePageLabelEl);
      return;
    }

    try {
      const offset = prePage * PRE_PAGE_SIZE;
      const { resp, payload } = await getJson(
        `/api/v1/runs/${encodeURIComponent(runId)}/pre_annotations?limit=${PRE_PAGE_SIZE}&offset=${offset}`,
      );
      if (!resp.ok || !payload?.ok) {
        hideTable(preTableEl, preBodyEl);
        setMessage(preMessageEl, payload?.error?.message || "Unable to load pre-annotation output.");
        updatePager(preTotalCount, PRE_PAGE_SIZE, prePage, prePagePrevEl, prePageNextEl, prePageLabelEl);
        return;
      }
      preTotalCount = Number.isFinite(payload?.data?.total_count) ? payload.data.total_count : 0;
      const rows = Array.isArray(payload?.data?.pre_annotations) ? payload.data.pre_annotations : [];
      if (rows.length === 0) {
        hideTable(preTableEl, preBodyEl);
        setMessage(preMessageEl, "Pre-annotation stage succeeded but no rows are available for current upload.");
        updatePager(preTotalCount, PRE_PAGE_SIZE, prePage, prePagePrevEl, prePageNextEl, prePageLabelEl);
        return;
      }
      renderPreRows(rows);
      setMessage(preMessageEl, "");
      updatePager(preTotalCount, PRE_PAGE_SIZE, prePage, prePagePrevEl, prePageNextEl, prePageLabelEl);
    } catch {
      hideTable(preTableEl, preBodyEl);
      setMessage(preMessageEl, "Unable to load pre-annotation output.");
      updatePager(preTotalCount, PRE_PAGE_SIZE, prePage, prePagePrevEl, prePageNextEl, prePageLabelEl);
    }
  }

  async function refreshClassifications(runId, stage) {
    const failed = stageFailureText(stage, "Classification failed.");
    if (failed) {
      hideTable(clsTableEl, clsBodyEl);
      setMessage(clsMessageEl, failed);
      updatePager(clsTotalCount, CLS_PAGE_SIZE, clsPage, clsPagePrevEl, clsPageNextEl, clsPageLabelEl);
      return;
    }
    if (!stage || stage.status !== "succeeded") {
      hideTable(clsTableEl, clsBodyEl);
      setMessage(clsMessageEl, stageStatusText(stage?.status, "Waiting for classification."));
      updatePager(clsTotalCount, CLS_PAGE_SIZE, clsPage, clsPagePrevEl, clsPageNextEl, clsPageLabelEl);
      return;
    }

    try {
      const offset = clsPage * CLS_PAGE_SIZE;
      const category = clsFilterEl ? String(clsFilterEl.value || "").trim() : "";
      const categoryParam = category ? `&category=${encodeURIComponent(category)}` : "";
      const { resp, payload } = await getJson(
        `/api/v1/runs/${encodeURIComponent(runId)}/classifications?limit=${CLS_PAGE_SIZE}&offset=${offset}${categoryParam}`,
      );
      if (!resp.ok || !payload?.ok) {
        hideTable(clsTableEl, clsBodyEl);
        setMessage(clsMessageEl, payload?.error?.message || "Unable to load classification output.");
        updatePager(clsTotalCount, CLS_PAGE_SIZE, clsPage, clsPagePrevEl, clsPageNextEl, clsPageLabelEl);
        return;
      }
      clsTotalCount = Number.isFinite(payload?.data?.total_count) ? payload.data.total_count : 0;
      const rows = Array.isArray(payload?.data?.classifications) ? payload.data.classifications : [];
      if (rows.length === 0) {
        hideTable(clsTableEl, clsBodyEl);
        setMessage(clsMessageEl, "Classification stage succeeded but no rows are available for current upload.");
        updatePager(clsTotalCount, CLS_PAGE_SIZE, clsPage, clsPagePrevEl, clsPageNextEl, clsPageLabelEl);
        return;
      }
      renderClassificationRows(rows);
      setMessage(clsMessageEl, "");
      updatePager(clsTotalCount, CLS_PAGE_SIZE, clsPage, clsPagePrevEl, clsPageNextEl, clsPageLabelEl);
    } catch {
      hideTable(clsTableEl, clsBodyEl);
      setMessage(clsMessageEl, "Unable to load classification output.");
      updatePager(clsTotalCount, CLS_PAGE_SIZE, clsPage, clsPagePrevEl, clsPageNextEl, clsPageLabelEl);
    }
  }

  async function refreshPredictions(runId, stage) {
    const failed = stageFailureText(stage, "Prediction failed.");
    if (failed) {
      cachedPredictionRows = [];
      hideTable(predTableEl, predBodyEl);
      setMessage(predMessageEl, failed);
      updatePager(predTotalCount, PRED_PAGE_SIZE, predPage, predPagePrevEl, predPageNextEl, predPageLabelEl);
      return;
    }
    if (!stage || stage.status !== "succeeded") {
      cachedPredictionRows = [];
      hideTable(predTableEl, predBodyEl);
      setMessage(predMessageEl, stageStatusText(stage?.status, "Waiting for prediction."));
      updatePager(predTotalCount, PRED_PAGE_SIZE, predPage, predPagePrevEl, predPageNextEl, predPageLabelEl);
      return;
    }

    try {
      const offset = predPage * PRED_PAGE_SIZE;
      const { resp, payload } = await getJson(
        `/api/v1/runs/${encodeURIComponent(runId)}/predictor_outputs?limit=${PRED_PAGE_SIZE}&offset=${offset}`,
      );
      if (!resp.ok || !payload?.ok) {
        cachedPredictionRows = [];
        hideTable(predTableEl, predBodyEl);
        setMessage(predMessageEl, payload?.error?.message || "Unable to load prediction output.");
        updatePager(predTotalCount, PRED_PAGE_SIZE, predPage, predPagePrevEl, predPageNextEl, predPageLabelEl);
        return;
      }
      predTotalCount = Number.isFinite(payload?.data?.total_count) ? payload.data.total_count : 0;
      const rows = Array.isArray(payload?.data?.predictor_outputs) ? payload.data.predictor_outputs : [];
      cachedPredictionRows = rows;
      const totalVariantRows = groupPredictionRows(rows);
      if (rows.length === 0 || totalVariantRows.length === 0) {
        hideTable(predTableEl, predBodyEl);
        setMessage(predMessageEl, "Prediction stage succeeded but no rows are available for current upload.");
        updatePager(predTotalCount, PRED_PAGE_SIZE, predPage, predPagePrevEl, predPageNextEl, predPageLabelEl);
        return;
      }
      const displayRows = predictionRowsForDisplay(rows);
      if (displayRows.length === 0) {
        hideTable(predTableEl, predBodyEl);
        setMessage(
          predMessageEl,
          "All prediction rows are currently hidden. Enable 'Show not_applicable rows' to view them.",
        );
        updatePager(predTotalCount, PRED_PAGE_SIZE, predPage, predPagePrevEl, predPageNextEl, predPageLabelEl);
        return;
      }
      renderPredictionRows(displayRows);
      if (displayRows.length !== totalVariantRows.length && !predShowNotApplicableEl?.checked) {
        setMessage(
          predMessageEl,
          `Showing ${displayRows.length} of ${totalVariantRows.length} variants (all predictors not_applicable hidden).`,
        );
      } else {
        setMessage(predMessageEl, "");
      }
      updatePager(predTotalCount, PRED_PAGE_SIZE, predPage, predPagePrevEl, predPageNextEl, predPageLabelEl);
    } catch {
      cachedPredictionRows = [];
      hideTable(predTableEl, predBodyEl);
      setMessage(predMessageEl, "Unable to load prediction output.");
      updatePager(predTotalCount, PRED_PAGE_SIZE, predPage, predPagePrevEl, predPageNextEl, predPageLabelEl);
    }
  }

  async function refreshVariantSummaries(runId, stage) {
    const failed = stageFailureText(stage, "Parser failed.");
    if (failed) {
      hideTable(summaryTableEl, summaryBodyEl);
      setMessage(summaryMessageEl, failed);
      updatePager(
        summaryTotalCount,
        SUMMARY_PAGE_SIZE,
        summaryPage,
        summaryPagePrevEl,
        summaryPageNextEl,
        summaryPageLabelEl,
      );
      return;
    }
    if (!stage || stage.status !== "succeeded") {
      hideTable(summaryTableEl, summaryBodyEl);
      setMessage(summaryMessageEl, stageStatusText(stage?.status, "Waiting for parser."));
      updatePager(
        summaryTotalCount,
        SUMMARY_PAGE_SIZE,
        summaryPage,
        summaryPagePrevEl,
        summaryPageNextEl,
        summaryPageLabelEl,
      );
      return;
    }

    try {
      const offset = summaryPage * SUMMARY_PAGE_SIZE;
      const completeness = summaryCompletenessFilterEl
        ? String(summaryCompletenessFilterEl.value || "").trim()
        : "";
      const completenessParam = completeness ? `&completeness=${encodeURIComponent(completeness)}` : "";
      const { resp, payload } = await getJson(
        `/api/v1/runs/${encodeURIComponent(runId)}/variant_summaries?limit=${SUMMARY_PAGE_SIZE}&offset=${offset}${completenessParam}`,
      );
      if (!resp.ok || !payload?.ok) {
        hideTable(summaryTableEl, summaryBodyEl);
        setMessage(summaryMessageEl, payload?.error?.message || "Unable to load variant summary.");
        updatePager(
          summaryTotalCount,
          SUMMARY_PAGE_SIZE,
          summaryPage,
          summaryPagePrevEl,
          summaryPageNextEl,
          summaryPageLabelEl,
        );
        return;
      }
      summaryTotalCount = Number.isFinite(payload?.data?.total_count) ? payload.data.total_count : 0;
      const rows = Array.isArray(payload?.data?.variant_summaries) ? payload.data.variant_summaries : [];
      if (rows.length === 0) {
        hideTable(summaryTableEl, summaryBodyEl);
        setMessage(summaryMessageEl, "Parser stage succeeded but no variants are available for summary.");
        updatePager(
          summaryTotalCount,
          SUMMARY_PAGE_SIZE,
          summaryPage,
          summaryPagePrevEl,
          summaryPageNextEl,
          summaryPageLabelEl,
        );
        return;
      }
      renderVariantSummaryRows(rows);
      setMessage(summaryMessageEl, "");
      updatePager(
        summaryTotalCount,
        SUMMARY_PAGE_SIZE,
        summaryPage,
        summaryPagePrevEl,
        summaryPageNextEl,
        summaryPageLabelEl,
      );
    } catch {
      hideTable(summaryTableEl, summaryBodyEl);
      setMessage(summaryMessageEl, "Unable to load variant summary.");
      updatePager(
        summaryTotalCount,
        SUMMARY_PAGE_SIZE,
        summaryPage,
        summaryPagePrevEl,
        summaryPageNextEl,
        summaryPageLabelEl,
      );
    }
  }

  function setDetailsMessage(text) {
    setMessage(detailsMessageEl, text);
  }

  function setPredictionsMessage(text) {
    setMessage(predictionsMessageEl, text);
  }

  function setEvidenceMessage(text) {
    setMessage(evidenceMessageEl, text);
  }

  function isCurrentVariantRequest(requestToken) {
    return requestToken === variantDetailsRequestSeq;
  }

  function stageSourceCompleteness(sourceKey) {
    const annotationStage = stageByName(latestStages, "annotation");
    const map =
      annotationStage?.stats?.evidence_source_completeness
      && typeof annotationStage.stats.evidence_source_completeness === "object"
        ? annotationStage.stats.evidence_source_completeness
        : {};
    return formatCompleteness(map?.[sourceKey]) || "\u2014";
  }

  function evidenceSourceLabel(sourceKey, output) {
    const sourceBase = output?.source ? String(output.source) : sourceKey;
    const details = output?.details;
    const sourceMode =
      details && typeof details === "object" ? String(details?.source_mode || "").trim() : "";
    if (!sourceMode) return sourceBase;
    return `${sourceBase} (${sourceMode})`;
  }

  function renderVariantEvidenceSource(sourceKey, row, target) {
    const output = row || null;
    const completeness = stageSourceCompleteness(sourceKey);
    const completenessLabel =
      completeness === "\u2014" ? completeness : statusLabel(completeness) || completeness;
    setStatusIndicator(
      target.completenessEl,
      completeness === "\u2014" ? "" : completeness,
      completenessLabel,
    );
    if (!output) {
      setStatusIndicator(target.outcomeEl, "not available", statusLabel("not available"));
      setText(target.reasonEl, "\u2014");
      setText(target.timestampEl, "\u2014");
      setText(target.sourceEl, sourceKey);
      if (target.idEl) setText(target.idEl, "\u2014");
      if (target.extraEl) setText(target.extraEl, "\u2014");
      return;
    }
    setStatusIndicator(
      target.outcomeEl,
      output?.outcome ?? "",
      statusLabel(output?.outcome) || output?.outcome || "\u2014",
    );
    setText(target.reasonEl, output?.reason_message || output?.reason_code || "\u2014");
    setText(target.timestampEl, output?.retrieved_at ? formatDateTime(output.retrieved_at) : "\u2014");
    setText(target.sourceEl, evidenceSourceLabel(sourceKey, output));
    if (target.idEl) setText(target.idEl, output?.idValue ?? "\u2014");
    if (target.extraEl) setText(target.extraEl, output?.extraValue ?? "\u2014");
  }

  async function refreshVariantEvidence(runId, variantId, requestToken) {
    if (!isCurrentVariantRequest(requestToken)) return;
    if (!variantId) {
      setEvidenceMessage("Variant ID unavailable.");
      return;
    }
    setEvidenceMessage("Loading evidence outputs...");

    try {
      const [dbsnpResult, clinvarResult, gnomadResult] = await Promise.all([
        getJson(`/api/v1/runs/${encodeURIComponent(runId)}/dbsnp_evidence?variant_id=${encodeURIComponent(variantId || "")}`),
        getJson(`/api/v1/runs/${encodeURIComponent(runId)}/clinvar_evidence?variant_id=${encodeURIComponent(variantId || "")}`),
        getJson(`/api/v1/runs/${encodeURIComponent(runId)}/gnomad_evidence?variant_id=${encodeURIComponent(variantId || "")}`),
      ]);
      if (!isCurrentVariantRequest(requestToken)) return;

      const resultList = [dbsnpResult, clinvarResult, gnomadResult];
      if (resultList.some((result) => result?.resp?.status === 404)) {
        setEvidenceMessage("No run found.");
        return;
      }
      const firstError = resultList.find((result) => !result?.resp?.ok || !result?.payload?.ok);
      if (firstError) {
        setEvidenceMessage(firstError.payload?.error?.message || "Unable to load evidence outputs.");
        return;
      }

      const dbsnpRows = Array.isArray(dbsnpResult?.payload?.data?.dbsnp_evidence)
        ? dbsnpResult.payload.data.dbsnp_evidence
        : [];
      const clinvarRows = Array.isArray(clinvarResult?.payload?.data?.clinvar_evidence)
        ? clinvarResult.payload.data.clinvar_evidence
        : [];
      const gnomadRows = Array.isArray(gnomadResult?.payload?.data?.gnomad_evidence)
        ? gnomadResult.payload.data.gnomad_evidence
        : [];

      const stage = dbsnpResult?.payload?.data?.stage || null;
      const stageStatus = stage?.status || null;
      if (dbsnpRows.length === 0 && clinvarRows.length === 0 && gnomadRows.length === 0) {
        if (stageStatus === "running") {
          setEvidenceMessage("Annotation running...");
        } else if (stageStatus === "failed") {
          setEvidenceMessage(stage?.error?.message || "Annotation failed.");
        } else if (stageStatus === "canceled") {
          setEvidenceMessage("Annotation was canceled.");
        } else {
          setEvidenceMessage("Not available for current upload yet.");
        }
      } else {
        setEvidenceMessage("");
      }

      const dbsnp = dbsnpRows[0] || null;
      renderVariantEvidenceSource("dbsnp", dbsnp ? { ...dbsnp, idValue: dbsnp?.rsid, extraValue: null } : null, {
        outcomeEl: evDbsnpOutcomeEl,
        completenessEl: evDbsnpCompletenessEl,
        idEl: evDbsnpRsidEl,
        extraEl: null,
        reasonEl: evDbsnpReasonEl,
        timestampEl: evDbsnpTimestampEl,
        sourceEl: evDbsnpSourceEl,
      });

      const clinvar = clinvarRows[0] || null;
      renderVariantEvidenceSource(
        "clinvar",
        clinvar
          ? { ...clinvar, idValue: clinvar?.clinvar_id, extraValue: clinvar?.clinical_significance }
          : null,
        {
          outcomeEl: evClinvarOutcomeEl,
          completenessEl: evClinvarCompletenessEl,
          idEl: evClinvarIdEl,
          extraEl: evClinvarSignificanceEl,
          reasonEl: evClinvarReasonEl,
          timestampEl: evClinvarTimestampEl,
          sourceEl: evClinvarSourceEl,
        },
      );

      const gnomad = gnomadRows[0] || null;
      renderVariantEvidenceSource(
        "gnomad",
        gnomad
          ? {
            ...gnomad,
            idValue: gnomad?.gnomad_variant_id,
            extraValue: gnomad?.global_af != null ? String(gnomad.global_af) : "\u2014",
          }
          : null,
        {
          outcomeEl: evGnomadOutcomeEl,
          completenessEl: evGnomadCompletenessEl,
          idEl: evGnomadIdEl,
          extraEl: evGnomadAfEl,
          reasonEl: evGnomadReasonEl,
          timestampEl: evGnomadTimestampEl,
          sourceEl: evGnomadSourceEl,
        },
      );
    } catch {
      if (!isCurrentVariantRequest(requestToken)) return;
      setEvidenceMessage("Unable to load evidence outputs.");
    }
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

    setStatusIndicator(predSiftOutcomeEl, "", "\u2014");
    setText(predSiftScoreEl, "\u2014");
    setText(predSiftLabelEl, "\u2014");
    setText(predSiftReasonEl, "\u2014");
    setText(predSiftTimestampEl, "\u2014");
    setText(predSiftSourceEl, "\u2014");

    setStatusIndicator(predPolyphen2OutcomeEl, "", "\u2014");
    setText(predPolyphen2ScoreEl, "\u2014");
    setText(predPolyphen2LabelEl, "\u2014");
    setText(predPolyphen2ReasonEl, "\u2014");
    setText(predPolyphen2TimestampEl, "\u2014");
    setText(predPolyphen2SourceEl, "\u2014");

    setStatusIndicator(predAlphamissenseOutcomeEl, "", "\u2014");
    setText(predAlphamissenseScoreEl, "\u2014");
    setText(predAlphamissenseLabelEl, "\u2014");
    setText(predAlphamissenseReasonEl, "\u2014");
    setText(predAlphamissenseTimestampEl, "\u2014");
    setText(predAlphamissenseSourceEl, "\u2014");

    setEvidenceMessage("");
    setStatusIndicator(evDbsnpOutcomeEl, "", "\u2014");
    setStatusIndicator(evDbsnpCompletenessEl, "", "\u2014");
    setText(evDbsnpRsidEl, "\u2014");
    setText(evDbsnpReasonEl, "\u2014");
    setText(evDbsnpTimestampEl, "\u2014");
    setText(evDbsnpSourceEl, "\u2014");

    setStatusIndicator(evClinvarOutcomeEl, "", "\u2014");
    setStatusIndicator(evClinvarCompletenessEl, "", "\u2014");
    setText(evClinvarIdEl, "\u2014");
    setText(evClinvarSignificanceEl, "\u2014");
    setText(evClinvarReasonEl, "\u2014");
    setText(evClinvarTimestampEl, "\u2014");
    setText(evClinvarSourceEl, "\u2014");

    setStatusIndicator(evGnomadOutcomeEl, "", "\u2014");
    setStatusIndicator(evGnomadCompletenessEl, "", "\u2014");
    setText(evGnomadIdEl, "\u2014");
    setText(evGnomadAfEl, "\u2014");
    setText(evGnomadReasonEl, "\u2014");
    setText(evGnomadTimestampEl, "\u2014");
    setText(evGnomadSourceEl, "\u2014");
  }

  if (offcanvasEl) {
    offcanvasEl.addEventListener("shown.bs.offcanvas", () => {
      pauseRefresh();
    });
    offcanvasEl.addEventListener("hidden.bs.offcanvas", () => {
      variantDetailsRequestSeq += 1;
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

  window.addEventListener("sp:run-changed", (evt) => {
    pausePolling = false;
    const nextRunId = evt?.detail?.run?.run_id ?? null;
    if (!nextRunId) {
      lastRunId = null;
      resetToNoRun();
      scheduleNextRefresh(1800);
      return;
    }
    const runChanged = nextRunId !== lastRunId;
    if (runChanged) {
      htmlArtifactsReady = false;
      lastArtifactsFetchedAt = 0;
      prePage = 0;
      preTotalCount = 0;
      clsPage = 0;
      clsTotalCount = 0;
      predPage = 0;
      predTotalCount = 0;
      summaryPage = 0;
      summaryTotalCount = 0;
      clsInputVcfPage = 0;
      clsInputVcfHasNext = false;
      clsInputVcfPosFilter = "";
      predInputVcfPage = 0;
      predInputVcfHasNext = false;
      predInputVcfPosFilter = "";
      annotationVcfPage = 0;
      annotationVcfHasNext = false;
      annotationVcfPosFilter = "";
      if (classificationInputVcfPosEl) classificationInputVcfPosEl.value = "";
      if (predictionInputVcfPosEl) predictionInputVcfPosEl.value = "";
      if (annotationVcfPosEl) annotationVcfPosEl.value = "";
      lastRunId = nextRunId;
    }
    void refresh();
  });

  window.addEventListener("sp:variant-result", (evt) => {
    const detail = evt?.detail ?? {};
    const runId = detail?.run_id ?? loadRunId();
    if (!runId || runId !== loadRunId()) return;
    if (inFlight) {
      pendingVariantRefreshStage = detail?.stage_name ?? pendingVariantRefreshStage;
      return;
    }
    void refreshForVariantResult(detail?.stage_name ?? null);
  });

  window.addEventListener("sp:task-queue-reset", () => {
    resetTaskQueueState();
  });

  function flushPendingVariantRefresh() {
    if (inFlight) return;
    if (!pendingVariantRefreshStage) return;
    const stageName = pendingVariantRefreshStage;
    pendingVariantRefreshStage = null;
    void refreshForVariantResult(stageName);
  }

  async function refreshForVariantResult(stageName) {
    if (pausePolling) return;
    const runId = loadRunId();
    if (!runId) return;
    if (inFlight) return;
    inFlight = true;
    let fallbackFullRefresh = false;

    try {
      const { resp, payload } = await getJson(`/api/v1/runs/${encodeURIComponent(runId)}/stages`);
      if (resp.status === 404) {
        resetToNoRun();
        return;
      }
      if (!resp.ok || !payload?.ok) {
        latestStages = [];
        const message = payload?.error?.message || "Unable to load stage outputs.";
        setMessage(finalMessageEl, message);
        return;
      }

      const stages = Array.isArray(payload?.data?.stages) ? payload.data.stages : [];
      latestStages = stages;
      renderFinalResult(stages);

      const parserStage = stageByName(stages, "parser");
      const preStage = stageByName(stages, "pre_annotation");
      const classificationStage = stageByName(stages, "classification");
      const predictionStage = stageByName(stages, "prediction");
      const annotationStage = stageByName(stages, "annotation");
      const reportingStage = stageByName(stages, "reporting");

      if (stageName === "parser") {
        renderParser(parserStage);
        await refreshVariantSummaries(runId, parserStage);
        return;
      }
      if (stageName === "pre_annotation") {
        await refreshPreAnnotations(runId, preStage);
        return;
      }
      if (stageName === "classification") {
        await refreshClassifications(runId, classificationStage);
        await refreshVariantSummaries(runId, parserStage);
        return;
      }
      if (stageName === "prediction") {
        await refreshPredictions(runId, predictionStage);
        await refreshVariantSummaries(runId, parserStage);
        return;
      }
      if (stageName === "annotation") {
        renderAnnotation(annotationStage);
        await refreshAnnotationOutput(runId, annotationStage);
        await refreshAnnotationEvidence(runId, annotationStage);
        await refreshVariantSummaries(runId, parserStage);
        return;
      }
      if (stageName === "reporting") {
        renderReporting(reportingStage);
        await refreshArtifacts(runId);
        return;
      }

      fallbackFullRefresh = true;
    } catch {
      setMessage(finalMessageEl, "Unable to load results right now.");
    } finally {
      inFlight = false;
      flushPendingVariantRefresh();
      if (fallbackFullRefresh) {
        void refresh();
      }
    }
  }

  async function openVariantDetails(row, triggerEl) {
    if (!offcanvasEl) return;
    if (!window.bootstrap?.Offcanvas) return;

    const runId = loadRunId();
    if (!runId) return;
    const requestToken = ++variantDetailsRequestSeq;
    const variantId = row?.variant_id || "";

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
    setEvidenceMessage("Loading evidence outputs...");
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

    if (!row?.consequence_category && variantId) {
      try {
        const { resp, payload } = await getJson(
          `/api/v1/runs/${encodeURIComponent(runId)}/classifications?variant_id=${encodeURIComponent(variantId)}`,
        );
        if (!isCurrentVariantRequest(requestToken)) return;
        if (resp.ok && payload?.ok) {
          const rows = Array.isArray(payload?.data?.classifications) ? payload.data.classifications : [];
          if (rows.length > 0) {
            const cls = rows[0];
            setText(clsCategoryEl, cls?.consequence_category ?? "\u2014");
            const clsReason =
              cls?.consequence_category === "unclassified"
                ? cls?.reason_message || cls?.reason_code || "Unclassified."
                : cls?.reason_message || "\u2014";
            setText(clsReasonEl, clsReason);
          }
        }
      } catch {
        if (!isCurrentVariantRequest(requestToken)) return;
      }
    }

    try {
      const { resp, payload } = await getJson(
        `/api/v1/runs/${encodeURIComponent(runId)}/pre_annotations?variant_id=${encodeURIComponent(variantId)}`,
      );
      if (!isCurrentVariantRequest(requestToken)) return;
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
      if (!isCurrentVariantRequest(requestToken)) return;
      setDetailsMessage("Unable to load variant details.");
    }

    try {
      const { resp, payload } = await getJson(
        `/api/v1/runs/${encodeURIComponent(runId)}/predictor_outputs?variant_id=${encodeURIComponent(variantId)}`,
      );
      if (!isCurrentVariantRequest(requestToken)) return;
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
          } else if (stageStatus === "succeeded" && row?.consequence_category === "other") {
            setPredictionsMessage("Prediction skipped for 'other' classification.");
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
              setStatusIndicator(target.outcomeEl, "not available", statusLabel("not available"));
              setText(target.scoreEl, "\u2014");
              setText(target.labelEl, "\u2014");
              setText(target.reasonEl, "\u2014");
              setText(target.timestampEl, "\u2014");
              setText(target.sourceEl, predictorKey);
              return;
            }
            setStatusIndicator(
              target.outcomeEl,
              output?.outcome ?? "",
              statusLabel(output?.outcome) || output?.outcome || "\u2014",
            );
            setText(target.scoreEl, output?.score != null ? String(output.score) : "\u2014");
            setText(target.labelEl, output?.label != null && output?.label !== "" ? String(output.label) : "\u2014");
            setText(target.reasonEl, output?.reason_message || output?.reason_code || "\u2014");
            setText(target.timestampEl, output?.created_at ? formatDateTime(output.created_at) : "\u2014");
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
      if (!isCurrentVariantRequest(requestToken)) return;
      setPredictionsMessage("Unable to load predictor outputs.");
    }

    await refreshVariantEvidence(runId, variantId, requestToken);
  }

  function resetToNoRun() {
    cachedPredictionRows = [];
    latestStages = [];
    htmlArtifactsReady = false;
    lastArtifactsFetchedAt = 0;
    prePage = 0;
    preTotalCount = 0;
    clsPage = 0;
    clsTotalCount = 0;
    predPage = 0;
    predTotalCount = 0;
    summaryPage = 0;
    summaryTotalCount = 0;
    if (clsFilterEl) clsFilterEl.value = "";
    if (summaryCompletenessFilterEl) summaryCompletenessFilterEl.value = "";
    if (annotationEvidenceFilterEl) annotationEvidenceFilterEl.value = "missense";
    setMessage(finalMessageEl, "Start a run to view partial and final results.");
    setMessage(parserMessageEl, "Start a run to see parser output.");
    setSummaryRows(parserSummaryEl, []);

    hideTable(preTableEl, preBodyEl);
    setMessage(preMessageEl, "Start a run to see pre-annotation output.");
    updatePager(preTotalCount, PRE_PAGE_SIZE, prePage, prePagePrevEl, prePageNextEl, prePageLabelEl);

    hideTable(clsTableEl, clsBodyEl);
    setMessage(clsMessageEl, "Start a run to see classification output.");
    updatePager(clsTotalCount, CLS_PAGE_SIZE, clsPage, clsPagePrevEl, clsPageNextEl, clsPageLabelEl);

    hideTable(predTableEl, predBodyEl);
    setMessage(predMessageEl, "Start a run to see prediction output.");
    updatePager(predTotalCount, PRED_PAGE_SIZE, predPage, predPagePrevEl, predPageNextEl, predPageLabelEl);

    setMessage(annotationMessageEl, "Start a run to see annotation output.");
    setSummaryRows(annotationSummaryEl, []);
    setMessage(annotationDiagnosticsMessageEl, "Start a run to see evidence diagnostics.");
    hideAnnotationDiagnosticsTable();
    setMessage(annotationVcfMessageEl, "Start a run to see annotated VCF preview.");
    hideAnnotationVcfTable();
    updateSimplePager(0, false, annotationVcfPagePrevEl, annotationVcfPageNextEl, annotationVcfPageLabelEl, "");
    setMessage(finalHtmlArtifactsMessageEl, "Start a run to see HTML summaries.");
    clearEl(finalHtmlArtifactsEl);

    setMessage(reportingMessageEl, "Start a run to see the reporting summary.");
    setSummaryRows(reportingSummaryEl, []);
    setSummaryRows(reportingSignificantEl, []);
    setSummaryRows(reportingEvidenceDiagnosticsEl, []);
    setMessage(summaryMessageEl, "Start a run to see the variant summary.");
    hideTable(summaryTableEl, summaryBodyEl);
    updatePager(
      summaryTotalCount,
      SUMMARY_PAGE_SIZE,
      summaryPage,
      summaryPagePrevEl,
      summaryPageNextEl,
      summaryPageLabelEl,
    );
    updateSimplePager(0, false, classificationInputVcfPagePrevEl, classificationInputVcfPageNextEl, classificationInputVcfPageLabelEl, "");
    updateSimplePager(0, false, predictionInputVcfPagePrevEl, predictionInputVcfPageNextEl, predictionInputVcfPageLabelEl, "");
  }

  if (predShowNotApplicableEl) {
    predShowNotApplicableEl.checked = false;
    predShowNotApplicableEl.addEventListener("change", () => {
      if (!cachedPredictionRows.length) return;
      const totalVariantRows = groupPredictionRows(cachedPredictionRows);
      const displayRows = predictionRowsForDisplay(cachedPredictionRows);
      if (displayRows.length === 0) {
        hideTable(predTableEl, predBodyEl);
        setMessage(
          predMessageEl,
          "All prediction rows are currently hidden. Enable 'Show not_applicable rows' to view them.",
        );
        return;
      }
      renderPredictionRows(displayRows);
      if (displayRows.length !== totalVariantRows.length && !predShowNotApplicableEl.checked) {
        setMessage(
          predMessageEl,
          `Showing ${displayRows.length} of ${totalVariantRows.length} variants (all predictors not_applicable hidden).`,
        );
      } else {
        setMessage(predMessageEl, "");
      }
    });
  }

  function maxPageIndex(totalCount, pageSize) {
    const total = Math.max(0, Number.isFinite(totalCount) ? totalCount : 0);
    const size = Math.max(1, Number.isFinite(pageSize) ? pageSize : 1);
    return total > 0 ? Math.max(0, Math.ceil(total / size) - 1) : 0;
  }

  function refreshPreForCurrentRun() {
    const runId = loadRunId();
    if (!runId) return;
    const stage = stageByName(latestStages, "pre_annotation");
    void refreshPreAnnotations(runId, stage);
  }

  function refreshClsForCurrentRun() {
    const runId = loadRunId();
    if (!runId) return;
    const stage = stageByName(latestStages, "classification");
    void refreshClassifications(runId, stage);
  }

  function refreshPredForCurrentRun() {
    const runId = loadRunId();
    if (!runId) return;
    const stage = stageByName(latestStages, "prediction");
    void refreshPredictions(runId, stage);
  }

  function refreshSummaryForCurrentRun() {
    const runId = loadRunId();
    if (!runId) return;
    const stage = stageByName(latestStages, "parser");
    void refreshVariantSummaries(runId, stage);
  }

  if (prePagePrevEl) {
    prePagePrevEl.addEventListener("click", () => {
      if (prePage <= 0) return;
      prePage -= 1;
      refreshPreForCurrentRun();
    });
  }
  if (prePageNextEl) {
    prePageNextEl.addEventListener("click", () => {
      const maxPage = maxPageIndex(preTotalCount, PRE_PAGE_SIZE);
      if (prePage >= maxPage) return;
      prePage += 1;
      refreshPreForCurrentRun();
    });
  }

  if (clsPagePrevEl) {
    clsPagePrevEl.addEventListener("click", () => {
      if (clsPage <= 0) return;
      clsPage -= 1;
      refreshClsForCurrentRun();
    });
  }
  if (clsPageNextEl) {
    clsPageNextEl.addEventListener("click", () => {
      const maxPage = maxPageIndex(clsTotalCount, CLS_PAGE_SIZE);
      if (clsPage >= maxPage) return;
      clsPage += 1;
      refreshClsForCurrentRun();
    });
  }

  if (predPagePrevEl) {
    predPagePrevEl.addEventListener("click", () => {
      if (predPage <= 0) return;
      predPage -= 1;
      refreshPredForCurrentRun();
    });
  }
  if (predPageNextEl) {
    predPageNextEl.addEventListener("click", () => {
      const maxPage = maxPageIndex(predTotalCount, PRED_PAGE_SIZE);
      if (predPage >= maxPage) return;
      predPage += 1;
      refreshPredForCurrentRun();
    });
  }

  if (summaryPagePrevEl) {
    summaryPagePrevEl.addEventListener("click", () => {
      if (summaryPage <= 0) return;
      summaryPage -= 1;
      refreshSummaryForCurrentRun();
    });
  }
  if (summaryPageNextEl) {
    summaryPageNextEl.addEventListener("click", () => {
      const maxPage = maxPageIndex(summaryTotalCount, SUMMARY_PAGE_SIZE);
      if (summaryPage >= maxPage) return;
      summaryPage += 1;
      refreshSummaryForCurrentRun();
    });
  }

  if (clsFilterEl) {
    clsFilterEl.addEventListener("change", () => {
      clsPage = 0;
      refreshClsForCurrentRun();
    });
  }
  if (summaryCompletenessFilterEl) {
    summaryCompletenessFilterEl.addEventListener("change", () => {
      summaryPage = 0;
      refreshSummaryForCurrentRun();
    });
  }
  if (annotationEvidenceFilterEl) {
    annotationEvidenceFilterEl.addEventListener("change", () => {
      lastEvidenceFetchedAt = 0;
      const runId = loadRunId();
      if (!runId) return;
      const stage = stageByName(latestStages, "annotation");
      void refreshAnnotationEvidence(runId, stage);
    });
  }
  if (annotationEvidenceOutcomeEl) {
    annotationEvidenceOutcomeEl.addEventListener("change", () => {
      lastEvidenceFetchedAt = 0;
      const runId = loadRunId();
      if (!runId) return;
      const stage = stageByName(latestStages, "annotation");
      void refreshAnnotationEvidence(runId, stage);
    });
  }

  if (classificationInputVcfSearchEl) {
    classificationInputVcfSearchEl.addEventListener("click", () => {
      clsInputVcfPosFilter = normalizePosFilter(classificationInputVcfPosEl?.value);
      if (classificationInputVcfPosEl) classificationInputVcfPosEl.value = clsInputVcfPosFilter;
      clsInputVcfPage = 0;
      refreshArtifactsForCurrentRun();
    });
  }
  if (classificationInputVcfClearEl) {
    classificationInputVcfClearEl.addEventListener("click", () => {
      clsInputVcfPosFilter = "";
      if (classificationInputVcfPosEl) classificationInputVcfPosEl.value = "";
      clsInputVcfPage = 0;
      refreshArtifactsForCurrentRun();
    });
  }
  if (classificationInputVcfPosEl) {
    classificationInputVcfPosEl.addEventListener("keydown", (evt) => {
      if (evt.key !== "Enter") return;
      evt.preventDefault();
      if (classificationInputVcfSearchEl) classificationInputVcfSearchEl.click();
    });
  }
  if (classificationInputVcfPagePrevEl) {
    classificationInputVcfPagePrevEl.addEventListener("click", () => {
      if (clsInputVcfPage <= 0) return;
      clsInputVcfPage -= 1;
      refreshArtifactsForCurrentRun();
    });
  }
  if (classificationInputVcfPageNextEl) {
    classificationInputVcfPageNextEl.addEventListener("click", () => {
      if (clsInputVcfPosFilter) return;
      if (!clsInputVcfHasNext) return;
      clsInputVcfPage += 1;
      refreshArtifactsForCurrentRun();
    });
  }

  if (predictionInputVcfSearchEl) {
    predictionInputVcfSearchEl.addEventListener("click", () => {
      predInputVcfPosFilter = normalizePosFilter(predictionInputVcfPosEl?.value);
      if (predictionInputVcfPosEl) predictionInputVcfPosEl.value = predInputVcfPosFilter;
      predInputVcfPage = 0;
      refreshArtifactsForCurrentRun();
    });
  }
  if (predictionInputVcfClearEl) {
    predictionInputVcfClearEl.addEventListener("click", () => {
      predInputVcfPosFilter = "";
      if (predictionInputVcfPosEl) predictionInputVcfPosEl.value = "";
      predInputVcfPage = 0;
      refreshArtifactsForCurrentRun();
    });
  }
  if (predictionInputVcfPosEl) {
    predictionInputVcfPosEl.addEventListener("keydown", (evt) => {
      if (evt.key !== "Enter") return;
      evt.preventDefault();
      if (predictionInputVcfSearchEl) predictionInputVcfSearchEl.click();
    });
  }
  if (predictionInputVcfPagePrevEl) {
    predictionInputVcfPagePrevEl.addEventListener("click", () => {
      if (predInputVcfPage <= 0) return;
      predInputVcfPage -= 1;
      refreshArtifactsForCurrentRun();
    });
  }
  if (predictionInputVcfPageNextEl) {
    predictionInputVcfPageNextEl.addEventListener("click", () => {
      if (predInputVcfPosFilter) return;
      if (!predInputVcfHasNext) return;
      predInputVcfPage += 1;
      refreshArtifactsForCurrentRun();
    });
  }

  if (annotationVcfSearchEl) {
    annotationVcfSearchEl.addEventListener("click", () => {
      annotationVcfPosFilter = normalizePosFilter(annotationVcfPosEl?.value);
      if (annotationVcfPosEl) annotationVcfPosEl.value = annotationVcfPosFilter;
      annotationVcfPage = 0;
      refreshAnnotationVcfForCurrentRun();
    });
  }
  if (annotationVcfClearEl) {
    annotationVcfClearEl.addEventListener("click", () => {
      annotationVcfPosFilter = "";
      if (annotationVcfPosEl) annotationVcfPosEl.value = "";
      annotationVcfPage = 0;
      refreshAnnotationVcfForCurrentRun();
    });
  }
  if (annotationVcfPosEl) {
    annotationVcfPosEl.addEventListener("keydown", (evt) => {
      if (evt.key !== "Enter") return;
      evt.preventDefault();
      if (annotationVcfSearchEl) annotationVcfSearchEl.click();
    });
  }
  if (annotationVcfPagePrevEl) {
    annotationVcfPagePrevEl.addEventListener("click", () => {
      if (annotationVcfPage <= 0) return;
      annotationVcfPage -= 1;
      refreshAnnotationVcfForCurrentRun();
    });
  }
  if (annotationVcfPageNextEl) {
    annotationVcfPageNextEl.addEventListener("click", () => {
      if (annotationVcfPosFilter) return;
      if (!annotationVcfHasNext) return;
      annotationVcfPage += 1;
      refreshAnnotationVcfForCurrentRun();
    });
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
        latestStages = [];
        const message = payload?.error?.message || "Unable to load stage outputs.";
        setMessage(finalMessageEl, message);
        scheduleNextRefresh(2500);
        return;
      }

      const stages = Array.isArray(payload?.data?.stages) ? payload.data.stages : [];
      latestStages = stages;
      renderFinalResult(stages);

      const parserStage = stageByName(stages, "parser");
      const preStage = stageByName(stages, "pre_annotation");
      const classificationStage = stageByName(stages, "classification");
      const predictionStage = stageByName(stages, "prediction");
      const annotationStage = stageByName(stages, "annotation");
      const reportingStage = stageByName(stages, "reporting");

      renderParser(parserStage);
      await refreshVariantSummaries(runId, parserStage);
      await refreshPreAnnotations(runId, preStage);
      await refreshClassifications(runId, classificationStage);
      await refreshPredictions(runId, predictionStage);
      renderAnnotation(annotationStage);
      await refreshAnnotationOutput(runId, annotationStage);
      await refreshAnnotationEvidence(runId, annotationStage);
      await refreshArtifacts(runId);
      renderReporting(reportingStage);
      refreshTooltips();

      if (isTerminalPipelineSnapshot(stages) && htmlArtifactsReady) {
        pausePolling = true;
        return;
      }
      scheduleNextRefresh(chooseNextInterval(stages));
    } catch {
      setMessage(finalMessageEl, "Unable to load results right now.");
      scheduleNextRefresh(2500);
    } finally {
      inFlight = false;
      flushPendingVariantRefresh();
    }
  }

  void refresh();
})();
