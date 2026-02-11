import { PageProps, Handlers } from "$fresh/server.ts";
import { getSession } from "../../utils/models/session.ts";
import DemoUserManagement from "../../islands/admin/DemoUserManagement.tsx";

interface AdminData {
  isAdmin: boolean;
  adminEmail?: string;
}

export const handler: Handlers<AdminData> = {
  async GET(_req, ctx) {
    const sessionId = (ctx.state as any).sessionId as string | undefined;
    
    if (!sessionId) {
      return new Response("", {
        status: 303,
        headers: { Location: "/admin/login" },
      });
    }

    const session = await getSession(sessionId);
    if (!session) {
      return new Response("", {
        status: 303,
        headers: { Location: "/admin/login" },
      });
    }

    const adminEmail = Deno.env.get("ADMIN_EMAIL");
    const isAdmin = session.username === adminEmail;

    if (!isAdmin) {
      return new Response("Forbidden", { status: 403 });
    }

    return ctx.render({ isAdmin: true, adminEmail: session.username });
  }
};

export default function AdminUsersPage({ data }: PageProps<AdminData>) {
  return (
    <div class="min-h-screen bg-gradient-to-br from-[#172217] to-[#186018] p-8">
      <div class="max-w-6xl mx-auto">
        <div class="bg-[#172217] border border-[#90C137]/30 rounded-2xl p-8 shadow-2xl">
          <div class="flex items-center justify-between mb-8">
            <div>
              <h1 class="text-3xl font-bold text-[#F8F6F0]">Admin Panel</h1>
              <p class="text-[#F8F6F0]/60 mt-1">Logged in as: {data.adminEmail}</p>
            </div>
            <a 
              href="/app/dashboard"
              class="px-4 py-2 bg-[#90C137]/10 border border-[#90C137]/30 text-[#90C137] rounded-lg hover:bg-[#90C137]/20 transition-colors"
            >
              Go to Dashboard
            </a>
          </div>

          {/* Create User Section */}
          <div class="bg-[#90C137]/5 border border-[#90C137]/20 rounded-lg p-6 mb-8">
            <h2 class="text-xl font-bold text-[#F8F6F0] mb-4">Create New User</h2>
            <form id="create-user-form" class="space-y-4">
              <div>
                <label class="block text-sm text-[#F8F6F0]/80 mb-1">Email</label>
                <input 
                  type="email" 
                  name="email"
                  required 
                  placeholder="user@example.com"
                  class="w-full p-3 bg-[#172217] border border-[#90C137]/30 rounded-lg text-[#F8F6F0] focus:border-[#90C137] outline-none"
                />
              </div>
              <div>
                <label class="block text-sm text-[#F8F6F0]/80 mb-1">Temporary Password</label>
                <input 
                  type="text" 
                  name="tempPassword"
                  required 
                  placeholder="Generate a secure password"
                  class="w-full p-3 bg-[#172217] border border-[#90C137]/30 rounded-lg text-[#F8F6F0] focus:border-[#90C137] outline-none"
                />
                <p class="text-xs text-[#F8F6F0]/50 mt-1">User will need to use this to log in</p>
              </div>
              <div>
                <label class="block text-sm text-[#F8F6F0]/80 mb-1">AI Model Tier</label>
                <select 
                  name="modelTier"
                  class="w-full p-3 bg-[#172217] border border-[#90C137]/30 rounded-lg text-[#F8F6F0] focus:border-[#90C137] outline-none"
                >
                  <option value="3b">3B Model (Default)</option>
                  <option value="7b">7B Model (More Powerful)</option>
                </select>
                <p class="text-xs text-[#F8F6F0]/50 mt-1">Assign users to different models for experimentation</p>
              </div>
              <button 
                type="submit"
                class="w-full py-3 bg-[#90C137] text-[#172217] rounded-lg font-bold hover:bg-[#a0d147] transition-colors"
              >
                Create User
              </button>
            </form>
            <div id="create-message" class="mt-4 hidden"></div>
          </div>

          {/* User List Section */}
          <div class="bg-[#90C137]/5 border border-[#90C137]/20 rounded-lg p-6 mb-8">
            <div class="flex justify-between items-center mb-4">
              <h2 class="text-xl font-bold text-[#F8F6F0]">Existing Users</h2>
              <button 
                id="refresh-button"
                class="px-4 py-2 bg-[#90C137]/20 border border-[#90C137]/30 text-[#90C137] rounded-lg hover:bg-[#90C137]/30 transition-colors text-sm"
              >
                 Refresh
              </button>
            </div>
            <div id="users-list" class="space-y-2">
              <p class="text-[#F8F6F0]/60 text-center py-4">Loading users...</p>
            </div>
          </div>

          {/* Demo Access Management Section */}
          <DemoUserManagement />

          {/* Instructions Section */}
          <div class="bg-[#90C137]/5 border border-[#90C137]/20 rounded-lg p-6 mt-8">
            <h2 class="text-xl font-bold text-[#F8F6F0] mb-4"> Instructions</h2>
            <ol class="space-y-2 text-[#F8F6F0]/80 text-sm">
              <li>1. <strong>Regular Users:</strong> Create users here. They can log in via "Member Login".</li>
              <li>2. <strong>Demo Access:</strong> Add emails/passwords below. Users log in via "Demo Access" with these credentials.</li>
              <li>3. Once verified, demo users create their own dummy account (Regular User) to use the app.</li>
            </ol>
          </div>
        </div>
      </div>

      <script dangerouslySetInnerHTML={{__html: `
        // Load users on page load
        loadUsers();

        // Create user form handler
        document.getElementById('create-user-form').addEventListener('submit', async (e) => {
          e.preventDefault();
          const form = e.target;
          const formData = new FormData(form);
          const email = formData.get('email');
          const tempPassword = formData.get('tempPassword');
          const modelTier = formData.get('modelTier');
          
          const messageDiv = document.getElementById('create-message');
          messageDiv.classList.remove('hidden');
          messageDiv.className = 'mt-4 p-3 rounded';
          messageDiv.textContent = 'Creating user...';
          messageDiv.classList.add('bg-blue-900/50', 'border', 'border-blue-500/50', 'text-blue-200');
          
          try {
            const res = await fetch('/api/admin/create-user', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ email, tempPassword, modelTier })
            });
            
            const data = await res.json();
            
            if (res.ok) {
              messageDiv.className = 'mt-4 p-3 rounded bg-green-900/50 border border-green-500/50 text-green-200';
              messageDiv.textContent = ' ' + data.message;
              form.reset();
              loadUsers(); // Refresh the list
            } else {
              messageDiv.className = 'mt-4 p-3 rounded bg-red-900/50 border border-red-500/50 text-red-200';
              messageDiv.textContent = ' ' + data.error;
            }
          } catch (err) {
            messageDiv.className = 'mt-4 p-3 rounded bg-red-900/50 border border-red-500/50 text-red-200';
            messageDiv.textContent = ' Failed to create user';
          }
        });

        // Refresh button handler
        document.getElementById('refresh-button').addEventListener('click', loadUsers);

        // Load users function
        async function loadUsers() {
          const listDiv = document.getElementById('users-list');
          listDiv.innerHTML = '<p class="text-[#F8F6F0]/60 text-center py-4">Loading users...</p>';
          
          try {
            const res = await fetch('/api/admin/list-users');
            const data = await res.json();
            
            if (res.ok && data.users) {
              if (data.users.length === 0) {
                listDiv.innerHTML = '<p class="text-[#F8F6F0]/60 text-center py-4">No users found</p>';
              } else {
                listDiv.innerHTML = data.users.map(user => \`
                  <div class="flex items-center justify-between p-4 bg-[#172217] border border-[#90C137]/20 rounded-lg">
                    <div class="flex-1">
                      <p class="text-[#F8F6F0] font-medium">\${user.username}</p>
                      <div class="flex gap-4 mt-1 text-xs text-[#F8F6F0]/60">
                        <span>Model: \${user.preferred_model_tier || '3b'}</span>
                        <span>Plan: \${user.plan_tier}</span>
                        <span>Created: \${new Date(user.createdAt).toLocaleDateString()}</span>
                      </div>
                    </div>
                    <div class="flex items-center gap-2">
                      \${user.username !== "admin" && user.username !== "${data.adminEmail}" ? \`
                        <button 
                          onclick="deleteUser('\${user.username}')"
                          class="px-3 py-1.5 bg-red-900/30 border border-red-500/30 text-red-400 rounded hover:bg-red-900/50 transition-colors text-sm"
                        >
                          Delete
                        </button>
                      \` : ''}
                    </div>
                  </div>
                \`).join('');
              }
            } else {
              listDiv.innerHTML = '<p class="text-red-400 text-center py-4">Failed to load users</p>';
            }
          } catch (err) {
            listDiv.innerHTML = '<p class="text-red-400 text-center py-4">Error loading users</p>';
          }
        }

        // Delete user function
        window.deleteUser = async function(username) {
          if (!confirm(\`Are you sure you want to delete user "\${username}"?\`)) {
            return;
          }
          
          try {
            const res = await fetch('/api/admin/delete-user', {
              method: 'POST',
              headers: { 'Content-Type': 'application/json' },
              body: JSON.stringify({ username })
            });
            
            const data = await res.json();
            
            if (res.ok) {
              loadUsers();
            } else {
              alert(' ' + data.error);
            }
          } catch (err) {
            alert(' Failed to delete user');
          }
        }
      `}} />
    </div>
  );
}
