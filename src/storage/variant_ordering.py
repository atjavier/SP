def variant_order_by(prefix: str = "") -> str:
    normalized = (prefix or "").strip()
    if normalized and not normalized.endswith("."):
        normalized += "."

    p = normalized
    return f"""
ORDER BY
  CASE
    WHEN {p}chrom GLOB '[0-9]*' THEN 0
    WHEN {p}chrom = 'X' THEN 1
    WHEN {p}chrom = 'Y' THEN 2
    WHEN {p}chrom = 'MT' THEN 3
    ELSE 4
  END,
  CASE
    WHEN {p}chrom GLOB '[0-9]*' THEN CAST({p}chrom AS INTEGER)
    WHEN {p}chrom = 'X' THEN 23
    WHEN {p}chrom = 'Y' THEN 24
    WHEN {p}chrom = 'MT' THEN 25
    ELSE 1000
  END,
  {p}pos ASC,
  {p}ref ASC,
  {p}alt ASC
""".strip()

