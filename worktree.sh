#!/bin/bash

# worktree.sh - Create a new worktree with isolated Docker environment and Claude session
# Usage: ./worktree.sh <worktree-name> [base-branch]

set -e

# Check if worktree name is provided
if [ $# -lt 1 ]; then
    echo "Usage: $0 <worktree-name> [base-branch]"
    echo "Example: $0 feature-auth mcp"
    exit 1
fi

WORKTREE_NAME="$1"
BASE_BRANCH="${2:-$(git branch --show-current)}"
PROJECT_NAME="metaphor"
WORKTREE_DIR="../${PROJECT_NAME}-${WORKTREE_NAME}"

echo "Creating worktree: ${WORKTREE_DIR}"
echo "Base branch: ${BASE_BRANCH}"

# Function to find next available port
find_next_port() {
    local base_port=$1
    local port=$base_port

    while lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1 || docker ps --format "table {{.Ports}}" | grep -q ":$port->"; do
        ((port++))
    done

    echo $port
}

# Function to scan existing worktrees for used ports
get_used_ports() {
    local ports=()

    # Look for existing .env files in sibling worktree directories
    for dir in ../${PROJECT_NAME}-*; do
        if [ -d "$dir" ] && [ -f "$dir/.env" ]; then
            local metaphor_port=$(grep "METAPHOR_PORT" "$dir/.env" 2>/dev/null | cut -d'=' -f2)
            local mongo_port=$(grep "MONGO_PORT" "$dir/.env" 2>/dev/null | cut -d'=' -f2)

            if [ -n "$metaphor_port" ]; then
                ports+=($metaphor_port)
            fi
            if [ -n "$mongo_port" ]; then
                ports+=($mongo_port)
            fi
        fi
    done

    printf '%s\n' "${ports[@]}" | sort -n | uniq
}

# Find available ports
echo "Finding available ports..."
USED_PORTS=($(get_used_ports))
METAPHOR_PORT=$(find_next_port 8001)
MONGO_PORT=$(find_next_port 27018)

# Ensure MongoDB port doesn't conflict with Metaphor port
while [[ " ${USED_PORTS[@]} " =~ " ${MONGO_PORT} " ]] || [ $MONGO_PORT -eq $METAPHOR_PORT ]; do
    ((MONGO_PORT++))
done

echo "Assigned ports: Metaphor=${METAPHOR_PORT}, MongoDB=${MONGO_PORT}"

# Create git worktree
echo "Creating git worktree..."

# Check if we're trying to use the current branch
CURRENT_BRANCH=$(git branch --show-current)
if [ "$BASE_BRANCH" = "$CURRENT_BRANCH" ]; then
    # Create a new branch for the worktree
    NEW_BRANCH="worktree-${WORKTREE_NAME}"
    echo "Current branch '$BASE_BRANCH' is in use. Creating new branch '$NEW_BRANCH'..."
    git worktree add -b "$NEW_BRANCH" "$WORKTREE_DIR" "$BASE_BRANCH"
else
    git worktree add "$WORKTREE_DIR" "$BASE_BRANCH"
fi

# Change to worktree directory
cd "$WORKTREE_DIR"

# Create .env file with unique ports
echo "Creating .env file..."
cat > .env << EOF
METAPHOR_PORT=${METAPHOR_PORT}
MONGO_PORT=${MONGO_PORT}
GOOGLE_CLIENT_ID=\${GOOGLE_CLIENT_ID}
GOOGLE_CLIENT_SECRET=\${GOOGLE_CLIENT_SECRET}
GITHUB_CLIENT_ID=\${GITHUB_CLIENT_ID}
GITHUB_CLIENT_SECRET=\${GITHUB_CLIENT_SECRET}
EOF

# Update .mcp.json to use the new port
echo "Updating .mcp.json configuration..."
if [ -f .mcp.json ]; then
    # Use sed to update the metaphor URL port
    sed -i "s|http://localhost:[0-9]\+/mcp/|http://localhost:${METAPHOR_PORT}/mcp/|g" .mcp.json
    echo "Updated .mcp.json to use port ${METAPHOR_PORT}"
fi

echo "Environment file created with ports: ${METAPHOR_PORT}, ${MONGO_PORT}"
echo "Worktree setup complete at: ${WORKTREE_DIR}"

# Optional auto-start functionality
read -p "Start Docker Compose and Claude session now? (y/N): " -n 1 -r
echo
if [[ $REPLY =~ ^[Yy]$ ]]; then
    echo "Starting Docker Compose..."
    docker compose up -d

    echo "Waiting for services to start..."
    sleep 5

    echo "Services started. Opening Claude session..."
    echo "You can access your services at:"
    echo "  Metaphor: http://localhost:${METAPHOR_PORT}/"
    echo "  MongoDB: localhost:${MONGO_PORT}"
    echo
    echo "Starting Claude..."
    exec claude
else
    echo
    echo "Manual startup:"
    echo "  cd ${WORKTREE_DIR}"
    echo "  docker compose up -d"
    echo "  claude"
    echo
    echo "Services will be available at:"
    echo "  Metaphor: http://localhost:${METAPHOR_PORT}/"
    echo "  MongoDB: localhost:${MONGO_PORT}"
fi