# Contributing to Xcash

We love contributions! Here's how to get started.

## Development Setup

1. **Fork the repository**
2. **Clone your fork**
   ```bash
   git clone https://github.com/YOUR_USERNAME/xcash.git
   cd xcash
   ```
3. **Set up environment**
   ```bash
   ./scripts/init_env.sh
   docker compose up -d
   ```

## Code Style

- **Python**: Follow PEP 8. Run `ruff check .` before committing.
- **JavaScript/React**: Follow the existing patterns in the codebase.
- **Documentation**: Write docs for any new feature.

## Pull Request Process

1. Create a feature branch: `git checkout -b feat/your-feature`
2. Make your changes and commit with clear messages
3. Push and open a PR against the `main` branch
4. Ensure all checks pass
5. Request review from maintainers

## Reporting Issues

- **Bug report**: Include steps to reproduce, expected vs actual behavior, and environment details (OS, Docker version, chain configuration).
- **Feature request**: Describe the use case and how it benefits Xcash users.

## Code of Conduct

Be respectful, constructive, and inclusive. We're building for the decentralized economy — let's reflect those values in our community.

## Questions?

Open a Discussion on GitHub or email tech@xca.sh