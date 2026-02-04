import { useEffect, useState } from "preact/hooks";

interface DemoUser {
  email: string;
  createdAt: string;
}

export default function DemoUserManagement() {
  const [users, setUsers] = useState<DemoUser[]>([]);
  const [loading, setLoading] = useState(true);
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [msg, setMsg] = useState("");

  const fetchUsers = async () => {
    try {
      const res = await fetch("/api/admin/demo-users");
      if (res.ok) {
        setUsers(await res.json());
      }
    } catch (e) {
      console.error(e);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchUsers();
  }, []);

  const handleCreate = async (e: Event) => {
    e.preventDefault();
    if (!email || !password) return;
    
    try {
      const res = await fetch("/api/admin/demo-users", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "create", email, password })
      });
      
      if (res.ok) {
        setMsg("✅ User added");
        setEmail("");
        setPassword("");
        fetchUsers();
      } else {
        setMsg("❌ Failed to add user");
      }
    } catch (e) {
      setMsg("❌ Error adding user");
    }
  };

  const handleDelete = async (emailToDelete: string) => {
    if (!confirm(`Revoke access for ${emailToDelete}?`)) return;
    
    try {
      const res = await fetch("/api/admin/demo-users", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ action: "delete", email: emailToDelete })
      });
      
      if (res.ok) {
        fetchUsers();
      }
    } catch (e) {
      console.error(e);
    }
  };

  return (
    <div class="bg-[#172217] border border-[#90C137]/30 rounded-lg p-6 mt-8">
      <h2 class="text-2xl font-bold text-[#F8F6F0] mb-6">Manage Demo Access</h2>
      
      {/* Create Form */}
      <form onSubmit={handleCreate} class="mb-8 p-4 bg-black/20 rounded-lg border border-[#90C137]/10">
        <h3 class="text-lg font-medium text-[#90C137] mb-4">Grant Access</h3>
        <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
          <input 
            type="email" 
            placeholder="Email Address"
            value={email}
            onInput={(e) => setEmail((e.target as HTMLInputElement).value)}
            class="bg-[#172217] border border-[#90C137]/30 rounded px-3 py-2 text-[#F8F6F0]"
            required
          />
          <input 
            type="text" 
            placeholder="Access Code / Password"
            value={password}
            onInput={(e) => setPassword((e.target as HTMLInputElement).value)}
            class="bg-[#172217] border border-[#90C137]/30 rounded px-3 py-2 text-[#F8F6F0]"
            required
          />
          <button 
            type="submit"
            class="bg-[#90C137] text-[#172217] font-bold rounded px-4 py-2 hover:bg-[#a0d147]"
          >
            Add User
          </button>
        </div>
        {msg && <p class="mt-2 text-sm text-[#F8F6F0]/70">{msg}</p>}
      </form>

      {/* List */}
      <div class="overflow-x-auto">
        <table class="w-full text-left border-collapse">
          <thead>
            <tr class="border-b border-[#90C137]/20">
              <th class="py-3 px-4 text-[#90C137] font-medium">Email</th>
              <th class="py-3 px-4 text-[#90C137] font-medium">Created At</th>
              <th class="py-3 px-4 text-[#90C137] font-medium text-right">Actions</th>
            </tr>
          </thead>
          <tbody>
            {loading ? (
              <tr><td colSpan={3} class="py-4 text-center text-[#F8F6F0]/50">Loading...</td></tr>
            ) : users.length === 0 ? (
              <tr><td colSpan={3} class="py-4 text-center text-[#F8F6F0]/50">No demo users found</td></tr>
            ) : (
              users.map(u => (
                <tr key={u.email} class="border-b border-[#90C137]/10 hover:bg-[#90C137]/5">
                  <td class="py-3 px-4 text-[#F8F6F0]">{u.email}</td>
                  <td class="py-3 px-4 text-[#F8F6F0]/70">{new Date(u.createdAt).toLocaleDateString()}</td>
                  <td class="py-3 px-4 text-right">
                    <button 
                      onClick={() => handleDelete(u.email)}
                      class="text-red-400 hover:text-red-300 text-sm font-medium"
                    >
                      Revoke
                    </button>
                  </td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}
