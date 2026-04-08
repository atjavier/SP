(() => {
  const STORAGE_KEY = "sp_sidebar_collapsed";
  const root = document.documentElement;
  const toggleBtn = document.getElementById("sidebar-toggle");
  const sidebar = document.getElementById("app-sidebar");

  if (!root || !toggleBtn || !sidebar) return;

  function isCollapsed() {
    return root.getAttribute("data-sidebar-collapsed") === "true";
  }

  function setToggleState(collapsed) {
    if (collapsed) {
      root.setAttribute("data-sidebar-collapsed", "true");
    } else {
      root.removeAttribute("data-sidebar-collapsed");
    }
    toggleBtn.setAttribute("aria-expanded", collapsed ? "false" : "true");
    toggleBtn.setAttribute(
      "aria-label",
      collapsed ? "Expand sidebar" : "Collapse sidebar",
    );
    try {
      localStorage.setItem(STORAGE_KEY, collapsed ? "true" : "false");
    } catch {
      // ignore storage failures
    }
  }

  let stored = null;
  try {
    stored = localStorage.getItem(STORAGE_KEY);
  } catch {
    stored = null;
  }
  setToggleState(stored === "true");
  root.setAttribute("data-sidebar-ready", "true");

  toggleBtn.addEventListener("click", () => {
    setToggleState(!isCollapsed());
  });
})();
