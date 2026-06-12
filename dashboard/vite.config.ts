import { cpSync, mkdirSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const METRICS_TARGET =
  process.env.CUPIDMQ_METRICS_TARGET ?? "http://127.0.0.1:9752";

const dashboardRoot = dirname(fileURLToPath(import.meta.url));
const faviconSrc = resolve(dashboardRoot, "../assets/icon.png");
const faviconDst = resolve(dashboardRoot, "public/favicon.png");

function cupidmqFavicon() {
  return {
    name: "cupidmq-favicon",
    buildStart() {
      mkdirSync(resolve(dashboardRoot, "public"), { recursive: true });
      cpSync(faviconSrc, faviconDst);
    },
  };
}

export default defineConfig({
  plugins: [cupidmqFavicon(), react()],
  server: {
    fs: { allow: [".."] },
    port: 5175,
    host: "0.0.0.0",
    proxy: {
      "/metrics": {
        target: METRICS_TARGET,
        changeOrigin: true,
      },
    },
  },
});
