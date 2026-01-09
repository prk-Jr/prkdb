#!/bin/bash
# Development environment setup for PrkDB
# Installs dependencies and configures the development environment

set -e

echo "====================================="
echo "PrkDB Development Setup"
echo "====================================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Check if Rust is installed
if ! command -v cargo &> /dev/null; then
    echo -e "${RED}Error: Rust is not installed${NC}"
    echo "Please install Rust from https://rustup.rs/"
    exit 1
fi

echo -e "${GREEN}✓ Rust is installed${NC}"
rustc --version
cargo --version
echo ""

# Check Rust version (require 1.70+)
RUST_VERSION=$(rustc --version | awk '{print $2}')
echo "Checking Rust version: $RUST_VERSION"

# Install required components
echo "Installing Rust components..."
rustup component add rustfmt clippy
echo -e "${GREEN}✓ Rust components installed${NC}"
echo ""

# Install cargo tools
echo "Installing development tools..."

# cargo-watch for auto-recompilation
if ! command -v cargo-watch &> /dev/null; then
    echo "Installing cargo-watch..."
    cargo install cargo-watch
else
    echo -e "${GREEN}✓ cargo-watch already installed${NC}"
fi

# cargo-edit for managing dependencies
if ! command -v cargo-add &> /dev/null; then
    echo "Installing cargo-edit..."
    cargo install cargo-edit
else
    echo -e "${GREEN}✓ cargo-edit already installed${NC}"
fi

# cargo-nextest for faster testing
if ! command -v cargo-nextest &> /dev/null; then
    echo "Installing cargo-nextest..."
    cargo install cargo-nextest
else
    echo -e "${GREEN}✓ cargo-nextest already installed${NC}"
fi

echo ""

# Build the project
echo "Building PrkDB..."
if cargo build; then
    echo -e "${GREEN}✓ Build successful${NC}"
else
    echo -e "${RED}✗ Build failed${NC}"
    exit 1
fi
echo ""

# Run tests
echo "Running tests..."
if cargo test --lib; then
    echo -e "${GREEN}✓ Tests passed${NC}"
else
    echo -e "${YELLOW}Warning: Some tests failed${NC}"
fi
echo ""

# Setup git hooks (if in a git repository)
if [ -d ".git" ]; then
    echo "Setting up git hooks..."
    
    # Pre-commit hook
    cat > .git/hooks/pre-commit << 'EOF'
#!/bin/bash
# Pre-commit hook for PrkDB

echo "Running pre-commit checks..."

# Format check
if ! cargo fmt -- --check; then
    echo "Error: Code is not formatted. Run 'cargo fmt' first."
    exit 1
fi

# Clippy check
if ! cargo clippy -- -D warnings; then
    echo "Error: Clippy found issues. Fix them before committing."
    exit 1
fi

echo "Pre-commit checks passed!"
EOF
    
    chmod +x .git/hooks/pre-commit
    echo -e "${GREEN}✓ Git hooks installed${NC}"
fi
echo ""

# Create .env file if it doesn't exist
if [ ! -f ".env" ]; then
    echo "Creating .env file..."
    cat > .env << 'EOF'
# PrkDB Development Environment
RUST_LOG=debug
RUST_BACKTRACE=1
EOF
    echo -e "${GREEN}✓ .env file created${NC}"
fi
echo ""

# Make scripts executable
echo "Making scripts executable..."
chmod +x scripts/*.sh
echo -e "${GREEN}✓ Scripts are executable${NC}"
echo ""

# Summary
echo "====================================="
echo "Setup Complete!"
echo "====================================="
echo ""
echo "Development tools installed:"
echo "  ✓ rustfmt (code formatting)"
echo "  ✓ clippy (linting)"
echo "  ✓ cargo-watch (auto-rebuild)"
echo "  ✓ cargo-edit (dependency management)"
echo "  ✓ cargo-nextest (fast testing)"
echo ""
echo "Useful commands:"
echo "  cargo build              - Build the project"
echo "  cargo test               - Run tests"
echo "  cargo bench              - Run benchmarks"
echo "  cargo fmt                - Format code"
echo "  cargo clippy             - Run linter"
echo "  cargo watch -x test      - Auto-run tests on changes"
echo "  ./scripts/integration_test.sh  - Run integration tests"
echo "  ./scripts/benchmark.sh         - Run benchmarks"
echo ""
echo "Examples:"
echo "  cargo run --example basic_consumer"
echo ""
echo -e "${GREEN}Happy coding!${NC}"