This is the additional instructions for the anime recommendation project.



Project file structure:
anime-mvp/
├── src/
│   ├── ingestion/          # Fetch Jikan API, load into RDS
│   ├── preprocessing/      # Glue job scripts
│   ├── models/             # Recommender + trend training
│   ├── agents/             # AutoGen agent definitions
│   └── ui/                 # Streamlit app
├── tests/                  # Unit tests
├── data/                   # Local SQLite artificial watch history
├── schemas/                # SQL schema files
├── .env                    # Environment variables (openAI keys, DB creds, AWS keys)
├── pyproject.toml          # Poetry config
├── poetry.lock             # Dependency lock file
└── additional_instructions.md          # This file

*Importatnt*: 

This project MUST be run inside a Conda environment. Activate the environment before running any scripts. Command to use: `conda activate anime-mvp`

Use poetry for dependency management. 









