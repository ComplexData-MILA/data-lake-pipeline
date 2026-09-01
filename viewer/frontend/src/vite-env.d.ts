/// <reference types="vite/client" />

interface ImportMetaEnv {
  /** Absolute API origin (e.g. https://api-data-lake-viewer.ai4.institute).
   *  Defaults to "/api" (same-origin, dev + nginx/Docker deployments). */
  readonly VITE_API_BASE?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
