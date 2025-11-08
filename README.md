<<<<<<< HEAD
# Superhero Analytics — Data Analyst Project

## Company

I work as a data analyst at **Hero Insight Analytics**. The company researches and visualizes data about superheroes, their abilities, physical characteristics, and publishers. We analyze a large dataset (10,000+ rows, 8 related tables) to discover patterns such as the strongest and tallest heroes, which publishers produce the most characters, and which abilities are most common.

## Project Overview

This project imports a superhero dataset into PostgreSQL, builds an ER diagram of the database (tables `superhero`, `hero_attribute`, `hero_power`, `alignment`, `publisher`, `race`, `gender`, `attribute`, `superpower`), writes SQL queries for 10 analytical topics (file `queries.sql`), and runs them using a Python script `pythonTest.py`.

![Main Analytics Screenshot](images/screenshot.png)

## How to Run the Project

1. Clone the repository:

   ```bash
   git clone https://github.com/yourusername/superhero-analytics.git
   cd superhero-analytics
   ```

2. Create a virtual environment and install dependencies:

   ```bash
   python -m venv venv
   source venv/bin/activate  # Linux/Mac
   venv\Scripts\activate     # Windows
   pip install -r requirements.txt
   ```

3. Create the PostgreSQL database `superheroes` and import the tables from `db/`.

4. Edit `pythonTest.py` to set your PostgreSQL password.

5. Run the Python script:

   ```bash
   python pythonTest.py
   ```

6. Query results will appear in the terminal.

## Tools and Resources

* PostgreSQL
* Python (pandas, psycopg2)
* Apache Superset
* GitHub
* ER diagram created in draw.io

## Repository Structure

```
.
├── db/
├── images/
├── queries.sql
├── pythonTest.py
├── requirements.txt
└── README.md
```

## Analytical Topics

1. List of superheroes (first 10 rows)
2. Top 20 heroes by total attributes
3. Number of heroes with each power
4. Distribution of heroes by alignment
5. 10 tallest heroes
6. 10 heaviest heroes
7. Heroes without attributes and powers
8. Heroes with the highest number of powers
9. Average attribute values by alignment
10. Distribution of heroes by race

<img width="1900" height="624" alt="image" src="https://github.com/user-attachments/assets/9ebd7101-9ba7-4256-81bc-77e861f515b5" />
=======
# SuperHeroes-Database
# SuperHeroesManagmentDB
>>>>>>> e2b815b (first commit)
