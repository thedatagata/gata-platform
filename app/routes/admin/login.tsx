import { Head } from "$fresh/runtime.ts";
import AdminLogin from "../../islands/admin/AdminLogin.tsx";

export default function AdminLoginPage() {
  return (
    <>
      <Head>
        <title>Admin Console | DATA_GATA</title>
        <meta name="robots" content="noindex, nofollow" />
      </Head>
      <AdminLogin />
    </>
  );
}
