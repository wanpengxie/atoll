Icon set for the Coagent xhs device extension

Files

- assets/icons/icon.svg: Square app icon (vector). Use this as the source to export PNGs for the extension.
- assets/icons/icon-maskable.svg: Maskable variant (PWA or rounded masks).
- assets/icons/logo-lockup.svg: Horizontal lockup for docs/screenshots.
- assets/icons/icon-128.png: Existing raster icon used by the current manifest.

Recommended PNG exports (for Chrome MV3)

- 16x16: assets/icons/icon-16.png
- 32x32: assets/icons/icon-32.png
- 48x48: assets/icons/icon-48.png
- 128x128: assets/icons/icon-128.png

How to export PNGs on macOS (sips)

1. Open icon.svg in Preview and export a high‑res PNG (e.g., 1024x1024) to /tmp/icon-1024.png.
2. Run:
   sips -z 128 128 /tmp/icon-1024.png --out assets/icons/icon-128.png
   sips -z 48 48 /tmp/icon-1024.png --out assets/icons/icon-48.png
   sips -z 32 32 /tmp/icon-1024.png --out assets/icons/icon-32.png
   sips -z 16 16 /tmp/icon-1024.png --out assets/icons/icon-16.png

How to export PNGs with ImageMagick (Linux)
convert -background none assets/icons/icon.svg -resize 128x128 assets/icons/icon-128.png
convert -background none assets/icons/icon.svg -resize 48x48 assets/icons/icon-48.png
convert -background none assets/icons/icon.svg -resize 32x32 assets/icons/icon-32.png
convert -background none assets/icons/icon.svg -resize 16x16 assets/icons/icon-16.png

Wiring into the extension

- wxt.config.ts already copies assets/icons/\* into the dist bundle under assets/icons/.
- To use the new icons in the manifest, add entries like:
  icons: {
  "16": "assets/icons/icon-16.png",
  "32": "assets/icons/icon-32.png",
  "48": "assets/icons/icon-48.png",
  "128": "assets/icons/icon-128.png"
  }

Notes

- Chrome requires raster icons (PNG). Keep icon.svg as your single source of truth and export PNG sizes as needed.
- The maskable variant improves appearance on platforms that apply rounded masks.
