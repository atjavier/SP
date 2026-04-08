(() => {
  const TOOLTIP_GLOSSARY = {
    "section.run_summary":
      "High-level metadata for the active run, including identifiers, status, and reference build.",
    "term.run_id":
      "Unique identifier used to fetch logs, stages, and results for this run.",
    "term.run_status":
      "Overall pipeline state for this run (queued, running, failed, or succeeded).",
    "term.reference_build":
      "Genome assembly used for annotation and evidence matching.",
    "term.completeness":
      "Indicates whether evidence is complete, partial, or unavailable.",
    "term.completeness_filter":
      "Filters variants by evidence completeness status.",
    "term.evidence_source":
      "The database or source checked for evidence.",
    "term.evidence_mode":
      "How evidence sources are queried (online, offline, or hybrid).",
    "section.pipeline_stages":
      "Ordered pipeline steps with status, duration, and actionable retries.",
    "section.run_logs":
      "Most recent log lines emitted by the pipeline for this run.",
    "term.live_updates":
      "Shows whether the UI is connected to streaming status and log updates.",
    "section.stage_results":
      "Browse outputs produced by each pipeline stage. Reports summarizes the full run.",
    "section.evidence_diagnostics":
      "High-level view of evidence source availability and completeness.",
    "section.variant_summary":
      "Table of per-variant classification, prediction, and evidence coverage.",
    "section.variant_details":
      "Per-variant evidence and predictor outputs with provenance.",
    "stage.parser":
      "Parses the uploaded VCF and stores SNV records for the run.",
    "stage.pre_annotation":
      "Derives local, deterministic context (base change, substitution class) for each variant.",
    "stage.classification":
      "Classifies variants (e.g., missense, synonymous) to route downstream tools.",
    "stage.prediction":
      "Runs pathogenicity predictors (SIFT, PolyPhen-2, AlphaMissense) and stores outputs.",
    "stage.annotation":
      "Runs evidence annotation (SnpEff plus dbSNP/ClinVar/gnomAD retrieval).",
    "stage.reporting":
      "Summarizes pipeline outcomes and evidence diagnostics for the run.",
    "artifact.classification_input_vcf":
      "Minimal VCF input generated for classification.",
    "artifact.prediction_input_vcf":
      "Minimal VCF input generated for prediction tools.",
    "artifact.prediction_vep_jsonl":
      "Raw VEP JSONL output used to derive predictor results.",
    "artifact.snpeff_vcf":
      "Annotated VCF produced by SnpEff.",
    "artifact.reports_summary":
      "High-level reporting summary for the run.",
    "status.queued":
      "Stage is waiting to start.",
    "status.running":
      "Stage is actively executing.",
    "status.succeeded":
      "Stage completed successfully with outputs persisted.",
    "status.failed":
      "Stage stopped due to an error; downstream stages did not run.",
    "status.partial":
      "Some expected outputs are missing; results are incomplete.",
    "status.unavailable":
      "Outputs are not available for the current run or upload.",
    "status.canceled":
      "Run or stage was canceled before completion.",
    "status.not_applicable":
      "This output does not apply to the current variant or stage.",
    "status.not_available":
      "No data is available for this field in the current run.",
    "status.not_found":
      "The evidence source did not return a match for this variant.",
    "status.not_computed":
      "The tool did not compute a value for this variant.",
    "status.disabled":
      "This output is disabled for the current run.",
    "evidence_mode.online":
      "Evidence sources are queried over the network.",
    "evidence_mode.offline":
      "Evidence sources are queried from local indexed data.",
    "evidence_mode.hybrid":
      "Try local sources first, then fall back to online when needed.",
    "evidence_policy.stop":
      "Stop the run if any evidence source fails after retries.",
    "evidence_policy.continue":
      "Continue the run and mark missing evidence as partial or unavailable.",
    "completeness.complete":
      "All required sources returned results for this variant.",
    "completeness.partial":
      "Some sources returned results; others are missing or failed.",
    "completeness.unavailable":
      "No valid evidence was retrieved for the source.",
    "provenance.source":
      "The tool or database that produced this field.",
    "provenance.retrieved":
      "When the value was retrieved or generated.",
  };

  const DEFAULT_TRIGGER = "hover focus";
  const DEFAULT_CONTAINER = document.body;

  function getGlossaryText(key) {
    if (!key) return "";
    return TOOLTIP_GLOSSARY[key] || "";
  }

  function applyTooltipAttributes(el, key) {
    if (!el || !key) return false;
    const text = getGlossaryText(key);
    if (!text) return false;
    const existingToggle = el.getAttribute("data-bs-toggle");
    if (!existingToggle || existingToggle === "tooltip") {
      el.setAttribute("data-bs-toggle", "tooltip");
    }
    el.setAttribute("data-bs-title", text);
    el.setAttribute("data-bs-trigger", DEFAULT_TRIGGER);
    el.setAttribute("data-bs-container", "body");
    return true;
  }

  function ensureTooltipInstance(el) {
    if (!el || !window.bootstrap?.Tooltip) return;
    const existing = window.bootstrap.Tooltip.getInstance(el);
    if (existing) {
      existing.update();
      return;
    }
    window.bootstrap.Tooltip.getOrCreateInstance(el, {
      trigger: DEFAULT_TRIGGER,
      container: DEFAULT_CONTAINER,
    });
  }

  function applyGlossary(root = document) {
    if (!root) return;
    const targets = root.querySelectorAll("[data-tooltip-key]");
    targets.forEach((el) => {
      const key = el.dataset.tooltipKey || "";
      if (!applyTooltipAttributes(el, key)) return;
      ensureTooltipInstance(el);
    });
  }

  function refresh(root = document) {
    applyGlossary(root);
  }

  window.SPTooltips = {
    glossary: TOOLTIP_GLOSSARY,
    applyGlossary,
    refresh,
    applyTooltipAttributes,
    ensureTooltipInstance,
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", () => refresh(document));
  } else {
    refresh(document);
  }
})();
