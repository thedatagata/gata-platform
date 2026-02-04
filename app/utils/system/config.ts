const isBuild = Deno.env.get("BUILD_PHASE") === "true";

function getEnv(key: string, fallback = ""): string {
  return isBuild ? fallback : (Deno.env.get(key) || fallback);
}

export const config = {
  admin: {
    email: getEnv("ADMIN_EMAIL", "thedatagata@gmail.com")
  },
  session: {
    secret: getEnv("SESSION_SECRET", "build-secret"),
    cookieName: "data_gata_session",
    maxAge: 86400,
  },
  oauth: {
    googleClientId: getEnv("GOOGLE_CLIENT_ID"),
    googleClientSecret: getEnv("GOOGLE_CLIENT_SECRET"),
  }
};