#!/bin/bash
# ============================================================================
# Build Script for CPT Automation Scripts Docker Image
# ============================================================================

set -e  # Exit on error

echo "🐳 Building CPT Automation Scripts Docker Image..."
echo "=================================================="

# Get the directory where this script is located
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

echo "📁 Project root: $PROJECT_ROOT"
echo "📁 Backend dir: $SCRIPT_DIR"

# Change to project root
cd "$PROJECT_ROOT"

# Build the Docker image
echo ""
echo "🔨 Building Docker image..."
docker build -f backend/Dockerfile -t cpt-scraper-image:latest .

echo ""
echo "✅ Docker image built successfully!"
echo ""
echo "📊 Image details:"
docker images | grep cpt-scraper-image

echo ""
echo "🎯 Next steps:"
echo "  1. Update .env file with your Supabase credentials"
echo "  2. Start the service: cd backend && docker-compose up -d"
echo "  3. View logs: docker-compose logs -f backend"
echo "  4. Test API: curl http://localhost:8000/health"
echo ""

