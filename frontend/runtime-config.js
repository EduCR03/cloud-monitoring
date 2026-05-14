// Optional runtime override.
// Example:
// window.CLOUDV2_API_BASE_URL = "https://api.seu-dominio.com";
(function (globalScope) {
  if (!globalScope) return;

  const LOCAL_STORAGE_KEY = "cloudv2.apiBaseUrl";
  const LOCAL_DEFAULT_API_BASE_URL = "http://127.0.0.1:8008";
  const PRODUCTION_DEFAULT_API_BASE_URL = "https://back-cloud-monitor.duckdns.org";

  function readLocalStorageOverride() {
    try {
      if (!globalScope.localStorage) return "";
      return String(globalScope.localStorage.getItem(LOCAL_STORAGE_KEY) || "").trim();
    } catch (err) {
      return "";
    }
  }

  function isLocalHost() {
    const hostname = String((globalScope.location || {}).hostname || "").trim().toLowerCase();
    return hostname === "localhost" || hostname === "127.0.0.1";
  }

  const explicitOverride = typeof globalScope.CLOUDV2_API_BASE_URL === "string"
    ? String(globalScope.CLOUDV2_API_BASE_URL || "").trim()
    : "";
  const localStorageOverride = readLocalStorageOverride();

  if (explicitOverride) {
    globalScope.CLOUDV2_API_BASE_URL = explicitOverride;
    return;
  }
  if (localStorageOverride) {
    globalScope.CLOUDV2_API_BASE_URL = localStorageOverride;
    return;
  }
  globalScope.CLOUDV2_API_BASE_URL = isLocalHost()
    ? LOCAL_DEFAULT_API_BASE_URL
    : PRODUCTION_DEFAULT_API_BASE_URL;
})(typeof window !== "undefined" ? window : undefined);
