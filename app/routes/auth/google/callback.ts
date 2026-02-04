import { Handlers } from "$fresh/server.ts";
import { handleCallback } from "@deno/kv-oauth";
import { oauthConfig } from "../../../utils/oauth.ts";
import { getUserByEmail, getUser, createUser, updateUser } from "../../../utils/models/user.ts";
import { createSession } from "../../../utils/models/session.ts";

export const handler: Handlers = {
  async GET(req) {
    try {
      const { response, tokens, sessionId: _sessionId } = await handleCallback(req, oauthConfig);
      
      // Get user info from Google
      const userInfoResp = await fetch("https://www.googleapis.com/oauth2/v3/userinfo", {
        headers: { Authorization: `Bearer ${tokens.accessToken}` },
      });
      
      if (!userInfoResp.ok) {
        throw new Error("Failed to fetch user info from Google");
      }
      
      const userInfo = await userInfoResp.json();
      const email = userInfo.email;
      const googleId = userInfo.sub;
      const _name = userInfo.name || email.split("@")[0];

      // Check if user exists
      let user = await getUserByEmail(email);
      
      if (!user) {
        // Create new user
        // Generate a random username or use email prefix
        let username = email.split("@")[0].replace(/[^a-zA-Z0-9_]/g, "_");
        
        // Ensure username is at least 3 chars
        if (username.length < 3) username = "user_" + username;

        // Check if username already exists, if so append random string
        const baseUsername = username;
        let counter = 1;
        while (await getUser(username)) {
           username = `${baseUsername}_${counter++}`;
        }
        
        user = await createUser(username, {
          email,
          googleId,
          plan_tier: "free",
        });
      }

      // Check security status
      if (user.securityRestricted) {
        return new Response("Account restricted for security reasons. Please contact support.", { status: 403 });
      }

      // Store/Update tokens for GCS access
      await updateUser(user.username, {
        googleAccessToken: tokens.accessToken,
        googleRefreshToken: tokens.refreshToken,
      });

      // Create our app session
      const session = await createSession(user.username);

      // Redirect to dashboard with our session cookie
      const headers = new Headers(response.headers);
      headers.set("Set-Cookie", `session_id=${session.sessionId}; Path=/; HttpOnly; Max-Age=${7 * 24 * 60 * 60}; SameSite=Lax`);
      headers.set("Location", "/app/dashboard");

      return new Response(null, {
        status: 303,
        headers,
      });
    } catch (error) {
      console.error("Google OAuth error:", error);
      return new Response("Authentication failed", { status: 500 });
    }
  },
};
