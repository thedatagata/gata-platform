// static/sw.js
importScripts('https://esm.sh/@mlc-ai/web-llm@0.2.72');

let handler;

self.addEventListener("activate", function (event) {
  handler = new MLCWebLLM.ServiceWorkerMLCEngineHandler();
  console.log("WebLLM Service Worker activated");
});
