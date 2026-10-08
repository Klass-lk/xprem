export function dashboardBasename(): string {
  // window.env is written by the Go server when it serves the dashboard; a
  // statically hosted build sets VITE_DASHBOARD_BASENAME instead.
  return window.env?.DASHBOARD_BASENAME || import.meta.env.VITE_DASHBOARD_BASENAME || '/dashboard';
}
