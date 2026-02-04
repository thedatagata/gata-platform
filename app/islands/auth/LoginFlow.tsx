import { useState } from "preact/hooks";

export default function LoginFlow() {
  const [mode, setMode] = useState<'login' | 'signup'>('login');
  const [credentials, setCredentials] = useState({ username: '', password: '', email: '' });
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const handleSubmit = async (e: Event) => {
    e.preventDefault();
    setLoading(true);
    setError(null);

    const endpoint = mode === 'login' ? '/api/auth/login' : '/api/auth/signup';

    try {
      const res = await fetch(endpoint, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(credentials)
      });

      const data = await res.json();

      if (!res.ok) {
        throw new Error(data.error || `${mode === 'login' ? 'Login' : 'Signup'} failed`);
      }

      // Redirect to dashboard on success
      globalThis.location.href = '/app/dashboard';

    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
    }
  };

  const handleGoogleLogin = () => {
    globalThis.location.href = '/auth/google/signin';
  };

  return (
    <div class="min-h-screen bg-[#050805] flex items-center justify-center p-6 relative overflow-hidden">
      {/* Animated Background Elements */}
      <div class="absolute top-[-10%] left-[-10%] w-[40%] h-[40%] bg-gata-green/10 rounded-full blur-[120px] animate-pulse" />
      <div class="absolute bottom-[-10%] right-[-10%] w-[40%] h-[40%] bg-gata-green/5 rounded-full blur-[120px] animate-pulse" style="animation-delay: 2s" />

      <div class="max-w-md w-full relative z-10">
        <div class="bg-gata-dark/60 backdrop-blur-2xl rounded-[2.5rem] shadow-2xl p-10 border border-gata-green/20">
          <div class="text-center mb-10">
            <div class="w-20 h-20 rounded-3xl overflow-hidden border-2 border-gata-green/30 mx-auto mb-6 p-1 bg-gata-dark/80 group transform transition-transform hover:scale-105 duration-500">
               <img
                src="/gata_app_utils/nerdy_alligator_headshot.png"
                alt="DATA_GATA"
                class="w-full h-full object-cover rounded-2xl"
              />
            </div>
            <h2 class="text-4xl font-black text-gata-cream italic tracking-tighter uppercase mb-2">
              {mode === 'login' ? 'Welcome Back' : 'Create Account'}
            </h2>
            <p class="text-xs text-gata-cream/40 font-medium uppercase tracking-[0.2em]">
              {mode === 'login' ? 'Resume your data discovery' : 'Start your journey with DATA_GATA'}
            </p>
          </div>

          {error && (
            <div class="mb-6 p-4 bg-red-500/10 border border-red-500/20 rounded-2xl text-red-400 text-xs font-bold uppercase tracking-wider text-center">
              {error}
            </div>
          )}

          <form onSubmit={handleSubmit} class="space-y-4">
            {mode === 'signup' && (
              <div>
                <label class="block text-[10px] font-black text-gata-green uppercase tracking-[0.2em] mb-2 ml-1">Email Address</label>
                <input 
                  type="email" 
                  required 
                  value={credentials.email}
                  placeholder="alex@company.com"
                  onInput={(e) => setCredentials({...credentials, email: (e.target as HTMLInputElement).value})}
                  class="w-full px-5 py-4 bg-gata-dark/40 border border-gata-green/10 rounded-2xl text-gata-cream placeholder:text-gata-cream/20 focus:border-gata-green/40 focus:bg-gata-dark/60 outline-none transition-all duration-300"
                />
              </div>
            )}
            <div>
              <label class="block text-[10px] font-black text-gata-green uppercase tracking-[0.2em] mb-2 ml-1">Username</label>
              <input 
                type="text" 
                required 
                value={credentials.username}
                placeholder="alligator_88"
                onInput={(e) => setCredentials({...credentials, username: (e.target as HTMLInputElement).value})}
                class="w-full px-5 py-4 bg-gata-dark/40 border border-gata-green/10 rounded-2xl text-gata-cream placeholder:text-gata-cream/20 focus:border-gata-green/40 focus:bg-gata-dark/60 outline-none transition-all duration-300"
              />
            </div>
            <div>
              <label class="block text-[10px] font-black text-gata-green uppercase tracking-[0.2em] mb-2 ml-1">Password</label>
              <input 
                type="password" 
                required 
                value={credentials.password}
                placeholder="••••••••"
                onInput={(e) => setCredentials({...credentials, password: (e.target as HTMLInputElement).value})}
                class="w-full px-5 py-4 bg-gata-dark/40 border border-gata-green/10 rounded-2xl text-gata-cream placeholder:text-gata-cream/20 focus:border-gata-green/40 focus:bg-gata-dark/60 outline-none transition-all duration-300"
              />
            </div>
            
            <button 
              type="submit" 
              disabled={loading}
              class="w-full py-5 bg-gata-green text-gata-dark rounded-2xl font-black uppercase tracking-[0.2em] text-xs hover:bg-[#a0d147] active:scale-[0.98] transition-all duration-300 shadow-[0_10px_20px_rgba(144,193,55,0.2)] disabled:opacity-50 mt-6"
            >
              {loading ? 'Processing...' : (mode === 'login' ? 'Sign In' : 'Create Account')}
            </button>
          </form>

          <div class="flex items-center my-8">
            <div class="flex-1 h-[1px] bg-gata-green/10"></div>
            <span class="px-4 text-[10px] font-bold text-gata-cream/20 uppercase tracking-[0.2em]">or continue with</span>
            <div class="flex-1 h-[1px] bg-gata-green/10"></div>
          </div>

          <button 
            type="button"
            onClick={handleGoogleLogin}
            class="w-full py-4 bg-gata-dark/80 border border-gata-green/20 text-gata-cream rounded-2xl font-bold text-sm flex items-center justify-center gap-3 hover:bg-gata-dark transition-all duration-300 hover:border-gata-green/40"
          >
            <svg class="w-5 h-5" viewBox="0 0 24 24">
              <path fill="currentColor" d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z" />
              <path fill="currentColor" d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z" />
              <path fill="currentColor" d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z" />
              <path fill="currentColor" d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z" />
            </svg>
            Google OAuth
          </button>

          <p class="text-center mt-8 text-[10px] font-bold uppercase tracking-[0.1em] text-gata-cream/30">
            {mode === 'login' ? 'New to DATA_GATA?' : 'Already have an account?'}
            <button 
              type="button"
              onClick={() => setMode(mode === 'login' ? 'signup' : 'login')}
              class="ml-2 text-gata-green hover:underline decoration-2 underline-offset-4"
            >
              {mode === 'login' ? 'Sign Up' : 'Log In'}
            </button>
          </p>
        </div>
      </div>
    </div>
  );
}

