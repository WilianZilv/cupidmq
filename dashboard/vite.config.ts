import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

const METRICS_TARGET =
  process.env.CUPIDMQ_METRICS_TARGET ?? "http://127.0.0.1:9752";

export default defineConfig({
  plugins: [react()],
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
