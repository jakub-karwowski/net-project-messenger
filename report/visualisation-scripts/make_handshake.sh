#!/bin/sh
# Renders handshake.puml into ../tex/figures/handshake_{simplex,duplex}.pdf.
# Needs plantuml and rsvg-convert on PATH (e.g. conda install -c conda-forge plantuml librsvg).
set -eu
here=$(cd "$(dirname "$0")" && pwd)
out="$here/../tex/figures"
tmp=$(mktemp -d)
trap 'rm -rf "$tmp"' EXIT
plantuml -tsvg -o "$tmp" "$here/handshake.puml"
for shape in simplex duplex; do
    rsvg-convert -f pdf -o "$out/handshake_$shape.pdf" "$tmp/handshake_$shape.svg"
    # Cairo stamps PDF 1.7 but uses no 1.6+ features; 1.5 avoids xdvipdfmx/pdfTeX warnings.
    sed -i '1s/^%PDF-1\.[67]/%PDF-1.5/' "$out/handshake_$shape.pdf"
done
