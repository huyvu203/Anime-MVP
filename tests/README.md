# Test Organization

This directory contains organized test files for the Anime MVP project.

## Test Categories

### 🤖 agents/
Tests for AutoGen agents:
- `test_user_interface_agent.py` - UI Agent natural language processing tests
- `test_data_retrieval_agent.py` - Data Retrieval Agent query execution tests  
- `test_athena_agent.py` - Athena integration tests

### 🏗️ infrastructure/
Tests for AWS infrastructure setup:
- `test_glue_deployment.py` - AWS Glue job deployment tests
- `test_quick_deployment.py` - Quick deployment verification tests
- `fix_glue_role.py` - IAM role configuration script
- `add_athena_permissions.py` - Athena permission setup script

### 🔄 integration/
Integration and workflow tests:
- `test_orchestration.py` - Agent orchestration tests
- `test_connection.py` - AWS connection tests
- `sequential_workflow.py` - Sequential workflow implementation

### 📊 data/
Data processing and API tests:
- `test_athena_queries.py` - Athena SQL query tests
- `test_local_etl.py` - Local ETL processing tests
- `test_jikan_api.py` - Jikan API integration tests
- `test_pipeline.py` - Data pipeline tests
- `explore_jikan_endpoints.py` - API endpoint exploration script

## Running Tests

### Run All Tests
```bash
poetry run python run_tests.py --all
```

### Run Specific Category
```bash
poetry run python run_tests.py --category agents
poetry run python run_tests.py --category infrastructure  
poetry run python run_tests.py --category integration
poetry run python run_tests.py --category data
```

### List Available Tests
```bash
poetry run python run_tests.py --list
```

### Check Environment Setup
```bash
poetry run python run_tests.py --check-env
```

### Verbose Output
```bash
poetry run python run_tests.py --all --verbose
```

## Test Environment

Tests require the following environment variables:
- `OPENAI_API_KEY` - For AutoGen agents
- `AWS_ACCESS_KEY_ID` - For AWS services
- `AWS_SECRET_ACCESS_KEY` - For AWS services

Load these from a `.env` file or set them in your environment.