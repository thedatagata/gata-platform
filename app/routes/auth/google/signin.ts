import { Handlers } from "$fresh/server.ts";
import { signIn } from "@deno/kv-oauth";
import { oauthConfig } from "../../../utils/oauth.ts";

export const handler: Handlers = {
  async GET(req) {
    return await signIn(req, oauthConfig);
  },
};
