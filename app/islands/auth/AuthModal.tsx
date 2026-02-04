import { useState } from "preact/hooks";

interface AuthModalProps {
  selectedPlan: "starter" | "smarter";
  onSuccess: () => void;
  onCancel: () => void;
}

export default function AuthModal({ selectedPlan, onSuccess, onCancel }: AuthModalProps) {
  const [mode, setMode] = useState<"signup" | "login">("signup");
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [confirmPassword, setConfirmPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");

  const handleSubmit = async (e: Event) => {
    e.preventDefault();
    setError("");

    if (mode === "signup" && password !== confirmPassword) {
      setError("Passwords do not match");
      return;
    }

    if (password.length < 6) {
      setError("Password must be at least 6 characters");
      return;
    }

    setLoading(true);

    try {
      const endpoint = mode === "signup" ? "/api/auth/signup" : "/api/auth/login";
      const body = mode === "signup"
        ? { username, password, plan_tier: selectedPlan === "smarter" ? "premium" : "free" }
        : { username, password };

      const res = await fetch(endpoint, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(body),
      });

      const data = await res.json();

      if (!res.ok) {
        throw new Error(data.error || `${mode} failed`);
      }

      console.log(`✅ ${mode} successful:`, data.user);
      onSuccess();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Authentication failed");
      setLoading(false);
    }
  };

  return (
    <div 
      className="fixed inset-0 bg-black/60 backdrop-blur-sm flex items-center justify-center z-50 p-4"
      onClick={onCancel}
    >
      <div 
        className="bg-gata-dark border-2 border-gata-green rounded-2xl shadow-2xl max-w-md w-full p-8"
        onClick={(e) => e.stopPropagation()}
      >
        <h2 className="text-3xl font-bold text-gata-cream mb-2">
          {mode === "signup" ? "Create Account" : "Welcome Back"}
        </h2>
        <p className="text-gata-cream/70 mb-6">
          {mode === "signup" 
            ? `Sign up for the ${selectedPlan === "smarter" ? "Smarter" : "Starter"} plan` 
            : "Login to continue"}
        </p>

        {/* Tab Toggle */}
        <div className="flex gap-2 mb-6">
          <button
            type="button"
            onClick={() => { setMode("signup"); setError(""); }}
            className={`flex-1 py-2 rounded-lg font-semibold transition-all ${
              mode === "signup"
                ? "bg-gata-green text-gata-dark"
                : "bg-gata-dark/40 text-gata-cream/60 hover:bg-gata-dark/60"
            }`}
          >
            Sign Up
          </button>
          <button
            type="button"
            onClick={() => { setMode("login"); setError(""); }}
            className={`flex-1 py-2 rounded-lg font-semibold transition-all ${
              mode === "login"
                ? "bg-gata-green text-gata-dark"
                : "bg-gata-dark/40 text-gata-cream/60 hover:bg-gata-dark/60"
            }`}
          >
            Login
          </button>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-sm font-medium text-gata-cream mb-1">
              Username
            </label>
            <input
              type="text"
              value={username}
              onInput={(e) => setUsername((e.target as HTMLInputElement).value)}
              placeholder="username123"
              required
              className="w-full px-4 py-2 bg-gata-dark/40 border border-gata-green/30 rounded-lg text-gata-cream placeholder-gata-cream/40 focus:border-gata-green focus:ring-2 focus:ring-gata-green/20 outline-none"
            />
          </div>

          <div>
            <label className="block text-sm font-medium text-gata-cream mb-1">
              Password
            </label>
            <input
              type="password"
              value={password}
              onInput={(e) => setPassword((e.target as HTMLInputElement).value)}
              placeholder="••••••••"
              required
              className="w-full px-4 py-2 bg-gata-dark/40 border border-gata-green/30 rounded-lg text-gata-cream placeholder-gata-cream/40 focus:border-gata-green focus:ring-2 focus:ring-gata-green/20 outline-none"
            />
          </div>

          {mode === "signup" && (
            <div>
              <label className="block text-sm font-medium text-gata-cream mb-1">
                Confirm Password
              </label>
              <input
                type="password"
                value={confirmPassword}
                onInput={(e) => setConfirmPassword((e.target as HTMLInputElement).value)}
                placeholder="••••••••"
                required
                className="w-full px-4 py-2 bg-gata-dark/40 border border-gata-green/30 rounded-lg text-gata-cream placeholder-gata-cream/40 focus:border-gata-green focus:ring-2 focus:ring-gata-green/20 outline-none"
              />
            </div>
          )}

          {error && (
            <div className="p-3 bg-red-500/20 border border-red-500/50 rounded-lg text-red-200 text-sm">
              {error}
            </div>
          )}

          <div className="flex gap-3 pt-2">
            <button
              type="submit"
              disabled={loading}
              className="flex-1 bg-gradient-to-r from-gata-green to-[#a0d147] text-gata-dark font-bold py-3 px-6 rounded-lg hover:opacity-90 transition-opacity disabled:opacity-50"
            >
              {loading ? "..." : mode === "signup" ? "Create Account" : "Login"}
            </button>
            <button
              type="button"
              onClick={onCancel}
              className="px-6 py-3 border border-gata-green/50 text-gata-cream rounded-lg hover:bg-gata-green/10 transition-colors"
            >
              Cancel
            </button>
          </div>
        </form>
      </div>
    </div>
  );
}
