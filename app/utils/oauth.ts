import { createGoogleOAuthConfig } from "@deno/kv-oauth";

const isBuildPhase = Deno.env.get("BUILD_PHASE") === "true";

if (isBuildPhase) {
  if (!Deno.env.get("GOOGLE_CLIENT_ID")) Deno.env.set("GOOGLE_CLIENT_ID", "MOCK");
  if (!Deno.env.get("GOOGLE_CLIENT_SECRET")) Deno.env.set("GOOGLE_CLIENT_SECRET", "MOCK");
}

export const oauthConfig = createGoogleOAuthConfig({
  redirectUri: `${Deno.env.get("BASE_URL") || "http://localhost:8000"}/auth/google/callback`,
  scope: [
    "openid", 
    "email", 
    "profile",
  ],
});
