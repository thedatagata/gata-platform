// static/sw.ts
import { ServiceWorkerMLCEngineHandler } from "@mlc-ai/web-llm";

let handler: ServiceWorkerMLCEngineHandler;

self.addEventListener("activate", function (event) {
  handler = new ServiceWorkerMLCEngineHandler();
  console.log("WebLLM Service Worker activated");
});
