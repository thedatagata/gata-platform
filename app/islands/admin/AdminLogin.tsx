import { useState } from "preact/hooks";

export default function AdminLogin() {
  const [credentials, setCredentials] = useState({ username: '', password: '' });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: Event) => {
    e.preventDefault();
    setLoading(true);
    setError(null);

    try {
      const res = await fetch('/api/auth/login', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(credentials)
      });

      const data = await res.json();

      if (!res.ok) {
        throw new Error(data.error || 'Login failed');
      }

      // Redirect to Admin Panel on success
      globalThis.location.href = '/admin/users';

    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div class="min-h-screen bg-[#0a0f0a] flex items-center justify-center p-4">
      <div class="max-w-md w-full bg-[#172217] border border-red-900/30 rounded-lg shadow-2xl p-8">
        <div class="text-center mb-8">
          <div class="w-16 h-16 bg-red-900/20 rounded-full flex items-center justify-center mx-auto mb-4 border border-red-900/50">
            <span class="text-2xl"></span>
          </div>
          <h2 class="text-2xl font-bold text-red-500">Admin Console</h2>
          <p class="text-gray-500 text-sm mt-2">Restricted Access Only</p>
        </div>
        
        {error && (
          <div class="mb-4 p-3 bg-red-900/20 border border-red-900/50 rounded text-red-400 text-sm text-center">
            {error}
          </div>
        )}

        <form onSubmit={handleSubmit} class="space-y-4">
          <div>
            <label class="block text-xs uppercase tracking-wider text-gray-500 mb-1">Admin Email</label>
            <input 
              type="text" 
              required 
              value={credentials.username}
              onInput={(e) => setCredentials({...credentials, username: (e.target as HTMLInputElement).value})}
              class="w-full p-3 bg-black/50 border border-gray-800 rounded text-gray-200 focus:border-red-900 focus:ring-1 focus:ring-red-900 outline-none transition-colors"
            />
          </div>
          <div>
            <label class="block text-xs uppercase tracking-wider text-gray-500 mb-1">Password</label>
            <input 
              type="password" 
              required 
              value={credentials.password}
              onInput={(e) => setCredentials({...credentials, password: (e.target as HTMLInputElement).value})}
              class="w-full p-3 bg-black/50 border border-gray-800 rounded text-gray-200 focus:border-red-900 focus:ring-1 focus:ring-red-900 outline-none transition-colors"
            />
          </div>
          
          <button 
            type="submit" 
            disabled={loading}
            class="w-full py-3 bg-red-900 hover:bg-red-800 text-white font-bold rounded transition-colors disabled:opacity-50 mt-4"
          >
            {loading ? 'Authenticating...' : 'Access Console'}
          </button>
        </form>

        <div class="mt-8 text-center">
          <a href="/" class="text-xs text-gray-600 hover:text-gray-400">
            ← Return to Public Site
          </a>
        </div>
      </div>
    </div>
  );
}
