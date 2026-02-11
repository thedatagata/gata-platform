import { Handlers } from "$fresh/server.ts";
import { getUserByGoogleId, updateUser } from "../../../utils/models/user.ts";
import { deleteSessionsForUser } from "../../../utils/models/session.ts";

/**
 * RISC Event Handler (Google Cross-Account Protection)
 * https://developers.google.com/identity/protocols/risc
 */
export const handler: Handlers = {
  async POST(req) {
    try {
      const authHeader = req.headers.get("Authorization");
      if (!authHeader?.startsWith("Bearer ")) {
        return new Response("Unauthorized", { status: 401 });
      }

      // In a production environment, you MUST verify the JWT signature 
      // using Google's public keys. For this demo, we'll parse the payload
      // to demonstrate the logic.
      const jwt = authHeader.split(" ")[1];
      const [_header, payloadBase64, _signature] = jwt.split(".");
      
      const payload = JSON.parse(new TextDecoder().decode(
        Uint8Array.from(atob(payloadBase64.replace(/-/g, '+').replace(/_/g, '/')), c => c.charCodeAt(0))
      ));

      // 1. Verify Issuer and Audience
      // issuer should be 'https://accounts.google.com'
      // audience should be your CLIENT_ID

      // 2. Extract Event
      const events = payload.events;
      if (!events) return new Response("No events found", { status: 400 });

      // Google RISC events use specific URIs
      const eventUris = Object.keys(events);
      
      for (const uri of eventUris) {
        const _eventData = events[uri];
        const googleId = payload.sub; // Subject is the Google User ID

        if (!googleId) continue;

        console.log(` [RISC] Received event ${uri} for Google ID: ${googleId}`);

        const user = await getUserByGoogleId(googleId);
        if (!user) {
          console.warn(` [RISC] User with Google ID ${googleId} not found in our system.`);
          continue;
        }

        // Logic based on RISC Standard Events
        // https://schemas.openid.net/secevent/risc/event-type/account-compromised
        if (uri.includes("account-compromised") || uri.includes("account-purged")) {
          await updateUser(user.username, {
            securityRestricted: true,
            lastSecurityEvent: uri
          });
          await deleteSessionsForUser(user.username);
        }
        
        // If account is disabled
        if (uri.includes("account-disabled")) {
            await deleteSessionsForUser(user.username);
        }
      }

      return new Response("OK", { status: 202 });
    } catch (error) {
      console.error(" [RISC] Error processing security event:", error);
      return new Response("Error", { status: 500 });
    }
  }
};
