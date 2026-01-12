#!/bin/bash

# ============================================================================
# Build script for optimized Docker image
# ============================================================================

set -e  # Exit on error

echo "🐳 Building optimized Docker image..."
echo "================================================"

# Change to backend directory
cd "$(dirname "$0")"

# Build the image with optimized Dockerfile
docker build \
    -t cpt-scraper-image:latest \
    -t cpt-scraper-image:optimized \
    -f Dockerfile.optimized \
    .

echo "================================================"
echo "✅ Build complete!"
echo ""
echo "Image tags:"
echo "  - cpt-scraper-image:latest"
echo "  - cpt-scraper-image:optimized"
echo ""
echo "Image size:"
docker images cpt-scraper-image:latest --format "{{.Repository}}:{{.Tag}} - {{.Size}}"
echo ""
echo "To run the container:"
echo "  docker-compose up -d"
echo ""

