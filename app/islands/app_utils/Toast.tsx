import { useState, useEffect } from "preact/hooks";

export interface ToastMessage {
  id: string;
  type: "success" | "error" | "info";
  message: string;
  duration?: number;
}

// Global toast state (simple event emitter pattern for islands)
const listeners: Set<(toast: ToastMessage) => void> = new Set();

export function showToast(
  message: string,
  type: ToastMessage["type"] = "info",
  duration = 4000,
) {
  const toast: ToastMessage = {
    id: `${Date.now()}-${Math.random().toString(36).slice(2)}`,
    type,
    message,
    duration,
  };
  listeners.forEach((fn) => fn(toast));
}

export default function ToastContainer() {
  const [toasts, setToasts] = useState<ToastMessage[]>([]);

  useEffect(() => {
    const handler = (toast: ToastMessage) => {
      setToasts((prev) => [...prev, toast]);
      if (toast.duration && toast.duration > 0) {
        setTimeout(() => {
          setToasts((prev) => prev.filter((t) => t.id !== toast.id));
        }, toast.duration);
      }
    };
    listeners.add(handler);
    return () => {
      listeners.delete(handler);
    };
  }, []);

  const dismiss = (id: string) => {
    setToasts((prev) => prev.filter((t) => t.id !== id));
  };

  const colorMap = {
    success: {
      bg: "bg-gata-green/20",
      border: "border-gata-green/30",
      text: "text-gata-green",
    },
    error: {
      bg: "bg-gata-red/20",
      border: "border-gata-red/50",
      text: "text-red-400",
    },
    info: {
      bg: "bg-gata-dark/80",
      border: "border-gata-green/10",
      text: "text-gata-cream/80",
    },
  };

  if (toasts.length === 0) return null;

  return (
    <div class="fixed top-6 right-6 z-[9999] space-y-3 max-w-sm">
      {toasts.map((toast) => {
        const colors = colorMap[toast.type];
        return (
          <div
            key={toast.id}
            class={`${colors.bg} ${colors.border} border backdrop-blur-xl rounded-2xl px-5 py-4 shadow-2xl animate-fadeIn flex items-start gap-3`}
          >
            <span class={`${colors.text} text-sm font-bold flex-1`}>
              {toast.message}
            </span>
            <button
              type="button"
              onClick={() => dismiss(toast.id)}
              class="text-gata-cream/20 hover:text-gata-cream/60 transition-colors text-xs"
            >
              x
            </button>
          </div>
        );
      })}
    </div>
  );
}
