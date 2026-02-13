import { Handlers } from "$fresh/server.ts";

export const handler: Handlers = {
  GET(_req, _ctx) {
    // Redirect to signup — after account creation the user flows into onboarding
    return new Response("", {
      status: 303,
      headers: { Location: "/auth/signin" },
    });
  },
};
