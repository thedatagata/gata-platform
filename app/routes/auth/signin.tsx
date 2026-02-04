import { Head } from "$fresh/runtime.ts";
import LoginFlow from "../../islands/auth/LoginFlow.tsx";

export default function SigninPage() {
  return (
    <>
      <Head>
        <title>Sign In | DATA_GATA</title>
      </Head>
      <LoginFlow />
    </>
  );
}
