// routes/app/dashboard.tsx
import { PageProps, Handlers } from "$fresh/server.ts";
import DashboardRouter from "../../islands/onboarding/DashboardRouter.tsx";
import { getSession } from "../../utils/models/session.ts";
import { getUser } from "../../utils/models/user.ts";

interface DashboardData {
  sessionId: string;
  isAllowed: boolean;
  email?: string;
  tenantSlug?: string;
}

export const handler: Handlers<DashboardData> = {
  async GET(_req, ctx) {
    const sessionId = ctx.state.sessionId as string | undefined;

    if (!sessionId) {
      return new Response("", {
        status: 303,
        headers: { Location: "/auth/signin" },
      });
    }

    const session = await getSession(sessionId);
    if (!session) {
       return new Response("", {
        status: 303,
        headers: { Location: "/auth/signin" },
      });
    }

    const user = await getUser(session.username);
    const isAllowed = true;

    return ctx.render({
      sessionId,
      isAllowed,
      email: user?.email || session.username,
      tenantSlug: user?.tenant_slug || undefined,
    });
  }
};

export default function DashboardPage({ data }: PageProps<DashboardData>) {
  const { sessionId, isAllowed, email, tenantSlug } = data;

  if (!isAllowed) {
    return (
      <div class="min-h-screen bg-gata-dark flex items-center justify-center p-4">
        <div class="max-w-md w-full bg-gata-dark border border-gata-green/30 rounded-2xl p-8 shadow-2xl">
          <div class="text-center">
            <h1 class="text-2xl font-bold text-gata-cream mb-2">Private Demo Access</h1>
            <p class="text-gata-cream/70 mb-6">
              Thanks for checking out Data Gata! This is currently a private demo environment.
            </p>
            <div class="bg-gata-green/5 border border-gata-green/20 rounded-lg p-4 mb-6">
              <p class="text-sm text-gata-green">
                Logged in as: <span class="font-semibold">{email}</span>
              </p>
            </div>
            <a
              href="/"
              class="block w-full py-3 bg-gata-green text-gata-dark font-bold rounded-lg hover:bg-[#a0d147] transition-colors"
            >
              Return Home
            </a>
          </div>
        </div>
      </div>
    );
  }

  return (
    <DashboardRouter
      sessionId={sessionId}
      tenantSlug={tenantSlug}
    />
  );
}
