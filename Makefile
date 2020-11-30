install:
	@poetry install

clean:
	@rm -rf build dist .eggs *.egg-info
	@find . -type d -name '.mypy_cache' -exec rm -rf {} +
	@find . -type d -name '__pycache__' -exec rm -rf {} +

black: clean
	@poetry run isort titan/ tests/
	@poetry run black titan/ tests/

lint:
	@poetry run mypy titan/

.PHONY: tests

tests:
	@poetry run python pytests drama/