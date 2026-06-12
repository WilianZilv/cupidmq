/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_CUPIDMQ_METRICS?: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
