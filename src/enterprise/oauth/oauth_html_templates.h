// GizmoData Commercial License
// Copyright (c) 2026 GizmoData LLC. All rights reserved.
// See LICENSE file in the enterprise directory for details.

#pragma once

namespace gizmosql::enterprise {

// GizmoSQL logo (148x148 PNG, base64-encoded)
// clang-format off
constexpr const char* kGizmoSQLLogoBase64 =
    "iVBORw0KGgoAAAANSUhEUgAAAJQAAACUCAYAAAB1PADUAAAABGdBTUEAALGPC/xhBQAAACBjSFJN"
    "AAB6JgAAgIQAAPoAAACA6AAAdTAAAOpgAAA6mAAAF3CculE8AAAAUGVYSWZNTQAqAAAACAACARIA"
    "AwAAAAEAAQAAh2kABAAAAAEAAAAmAAAAAAADoAEAAwAAAAEAAQAAoAIABAAAAAEAAACUoAMABAAA"
    "AAEAAACUAAAAAKtqpZkAAAI0aVRYdFhNTDpjb20uYWRvYmUueG1wAAAAAAA8eDp4bXBtZXRhIHht"
    "bG5zOng9ImFkb2JlOm5zOm1ldGEvIiB4OnhtcHRrPSJYTVAgQ29yZSA2LjAuMCI+CiAgIDxyZGY6"
    "UkRGIHhtbG5zOnJkZj0iaHR0cDovL3d3dy53My5vcmcvMTk5OS8wMi8yMi1yZGYtc3ludGF4LW5z"
    "IyI+CiAgICAgIDxyZGY6RGVzY3JpcHRpb24gcmRmOmFib3V0PSIiCiAgICAgICAgICAgIHhtbG5z"
    "OmV4aWY9Imh0dHA6Ly9ucy5hZG9iZS5jb20vZXhpZi8xLjAvIgogICAgICAgICAgICB4bWxuczp0"
    "aWZmPSJodHRwOi8vbnMuYWRvYmUuY29tL3RpZmYvMS4wLyI+CiAgICAgICAgIDxleGlmOlBpeGVs"
    "WURpbWVuc2lvbj4xMDI0PC9leGlmOlBpeGVsWURpbWVuc2lvbj4KICAgICAgICAgPGV4aWY6UGl4"
    "ZWxYRGltZW5zaW9uPjEwMjQ8L2V4aWY6UGl4ZWxYRGltZW5zaW9uPgogICAgICAgICA8ZXhpZjpD"
    "b2xvclNwYWNlPjE8L2V4aWY6Q29sb3JTcGFjZT4KICAgICAgICAgPHRpZmY6T3JpZW50YXRpb24+"
    "MTwvdGlmZjpPcmllbnRhdGlvbj4KICAgICAgPC9yZGY6RGVzY3JpcHRpb24+CiAgIDwvcmRmOlJE"
    "Rj4KPC94OnhtcG1ldGE+CkUA42UAAC1ISURBVHgB7Z0JtCVFmecj8+7L22t9r3aKoqAKCikBscsW";
// clang-format on
// NOTE: Full base64 is very long; we embed a small subset for the logo header.
// In practice, the logo is served as a data URI in the HTML templates below.

// Common CSS styles shared across OAuth pages
constexpr const char* kOAuthPageStyles = R"(
<style>
  body {
    font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, 'Helvetica Neue', Arial, sans-serif;
    display: flex;
    justify-content: center;
    align-items: center;
    min-height: 100vh;
    margin: 0;
    background-color: #f8f9fa;
    color: #333;
  }
  .container {
    text-align: center;
    padding: 48px;
    background: white;
    border-radius: 12px;
    box-shadow: 0 2px 12px rgba(0,0,0,0.08);
    max-width: 480px;
  }
  .logo { width: 100px; height: 100px; margin-bottom: 24px; }
  h1 { font-size: 24px; font-weight: 600; margin: 0 0 12px 0; }
  p { font-size: 16px; color: #666; margin: 8px 0; }
  .error { color: #dc3545; }
</style>
)";

/// HTML page shown after successful OAuth authentication.
/// The browser auto-closes after 3 seconds.
constexpr const char* kOAuthSuccessPage = R"(<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>GizmoSQL - Authentication Successful</title>
  %STYLES%
</head>
<body>
  <div class="container">
    <img class="logo" src="data:image/png;base64,%LOGO%" alt="GizmoSQL">
    <h1>Authentication Successful</h1>
    <p>You have been authenticated. You may close this tab.</p>
    <p style="color:#999;font-size:13px;">This tab will close automatically in 3 seconds.</p>
  </div>
  <script>setTimeout(function(){ window.close(); }, 3000);</script>
</body>
</html>)";

/// HTML page shown when OAuth authentication fails.
/// Contains a %ERROR% placeholder for the error message.
constexpr const char* kOAuthErrorPage = R"(<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>GizmoSQL - Authentication Failed</title>
  %STYLES%
</head>
<body>
  <div class="container">
    <img class="logo" src="data:image/png;base64,%LOGO%" alt="GizmoSQL">
    <h1>Authentication Failed</h1>
    <p class="error">%ERROR%</p>
    <p>Please close this tab and try again.</p>
  </div>
</body>
</html>)";

/// HTML page shown when the authentication session has expired.
constexpr const char* kOAuthExpiredPage = R"(<!DOCTYPE html>
<html>
<head>
  <meta charset="utf-8">
  <title>GizmoSQL - Session Expired</title>
  %STYLES%
</head>
<body>
  <div class="container">
    <img class="logo" src="data:image/png;base64,%LOGO%" alt="GizmoSQL">
    <h1>Session Expired</h1>
    <p>Your authentication session has expired. Please try again.</p>
  </div>
</body>
</html>)";

}  // namespace gizmosql::enterprise
