# Contributing

Thanks for your interest in OnlyMetrix.

## Bug Reports

Open an issue with:
- What you expected
- What happened
- Python version and `onlymetrix` version (`pip show onlymetrix`)
- Minimal reproduction code

## Feature Requests

Open an issue describing the use case. Include what you're trying to achieve, not just the feature you want.

## Development

```bash
git clone https://github.com/dreynow/onlymetrix-python
cd onlymetrix-python
pip install -e ".[dev]"
pytest tests/ -q
```

## Pull Requests

- One feature per PR
- Include tests
- Run `pytest tests/ -q` before submitting
