# Viewing Documentation Locally

This guide shows you how to preview the Docsify documentation on your local machine.

## Quick Start

### Option 1: Python (Simplest)

```bash
cd docs
python3 -m http.server 3000
```

Then open: http://localhost:3000

### Option 2: Node.js

```bash
# Install http-server (one time)
npm install -g http-server

# Run server
cd docs
http-server -p 3000
```

Then open: http://localhost:3000

### Option 3: Docsify CLI (Best for development)

```bash
# Install docsify-cli (one time)
npm install -g docsify-cli

# Run server with hot reload
docsify serve docs
```

Then open: http://localhost:3000

## Features

- Live reload (with docsify-cli)
- Search functionality
- Dark/Light theme toggle
- Code syntax highlighting
- Responsive design

## Troubleshooting

**Port already in use?**
```bash
# Use different port
python3 -m http.server 3001
```

**Browser cache issues?**
- Press Cmd+Shift+R (Mac) or Ctrl+Shift+R (Windows/Linux)
- Or open in incognito/private window

**Files not updating?**
- Clear browser cache
- Use docsify-cli for auto-reload

## Next Steps

- Edit `.md` files in `docs/` directory
- Changes reflect immediately (with auto-reload)
- Push to GitHub to deploy to GitHub Pages
