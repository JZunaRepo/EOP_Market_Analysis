from sqlalchemy import create_engine, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
import pandas as pd
import config

engine = create_engine(f"postgresql+psycopg2://{config.user}:{config.password}@{config.host}:{config.port}/{config.dbname}")
Base = automap_base()
Base.prepare(engine, reflect=True)

jobs = Base.classes.jobs
salaries = Base.classes.salaries
skills = Base.classes.skills

session = Session(engine)
engine.dispose()

jobs_result = session.query(jobs)
salaries_result = session.query(salaries)
skills_result = session.query(skills)

jobs_df = pd.read_sql(jobs_result.statement, con=engine.connect())
salaries_df = pd.read_sql(salaries_result.statement, con=engine.connect())
skills_df = pd.read_sql(skills_result.statement, con=engine.connect())

merge_1 = pd.merge(left=salaries_df, right=jobs_df, how="left", on="id")
final_merge = pd.merge(left=merge_1, right=skills_df, how="left", on="id")

final_merge.to_csv('data/joined_data.csv')