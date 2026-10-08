/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_DASHBOARD_BASENAME?: string;
}

interface Window {
  env?: {
    VITE_OTA_API_URL?: string;
    DASHBOARD_BASENAME?: string;
  };
}

