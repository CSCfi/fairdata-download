# Testing

## Unit Tests

Unit tests can be run locally with coverage:

```
coverage run -m pytest
```

After running tests, coverage report can be displayed with:

```
coverage report -m
```

## End to End Tests

End to end tests can be run with the following command:

```
coverage run -m pytest tests/e2e
```
