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
    "Rj4KPC94OnhtcG1ldGE+CkUA42UAAC1ISURBVHgB7Z0JtCVFmecj8+7L22t9r3aKoqAKCikBscsW"
    "GESRhp4Gl5lWx1HPMLaj9nGbdsSRcmhxWltcWo4L47CoiDIOuNIgsh1RWxsBgaJ4VAG1b6/efvcl"
    "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"
    "SUVORK5CYII="
    ;
// clang-format on

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
