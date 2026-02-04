// components/AdminNav.tsx
export default function AdminNav() {
  return (
    <nav class="bg-[#172217] text-white shadow">
      <div class="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div class="flex justify-between h-16">
          <div class="flex">
            <div class="flex-shrink-0 flex items-center">
              <a href="/" class="flex items-center space-x-2">
                <div class="w-8 h-8 rounded-full overflow-hidden border-2 border-[#90C137]">
                  <img
                    src="/gata_app_utils/nerdy_alligator_headshot.png"
                    alt="DATA_GATA Logo"
                    class="w-full h-full object-cover"
                  />
                </div>
                <span class="font-bold">
                  DATA_<span class="text-[#90C137]">GATA</span> Admin
                </span>
              </a>
            </div>

            <div class="hidden sm:ml-6 sm:flex sm:space-x-8">
              <a
                href="/admin/contacts"
                class="border-[#90C137] text-white inline-flex items-center px-1 pt-1 border-b-2 text-sm font-medium"
              >
                Contact Submissions
              </a>
            </div>
          </div>

          <div class="hidden sm:ml-6 sm:flex sm:items-center">
            {/* Replace the form with a direct link */}
            <a
              href="/admin/oauth/signout"
              class="px-3 py-2 text-sm font-medium rounded-md text-[#172217] bg-[#90C137] hover:bg-[#7dab2a]"
            >
              Logout
            </a>
          </div>
        </div>
      </div>
    </nav>
  );
}