from sqlalchemy import create_engine, func
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
import pandas as pd
import config

x = config.ps
engine = create_engine(f"postgresql+psycopg2://postgres:{x}@localhost:5432/epi")
Base = automap_base()
Base.prepare(engine, reflect=True)
Base.classes.keys()
epi_country = Base.classes.epi_country
session = Session(engine)
results_epi = session.query(epi_country.code, epi_country.iso3v10, epi_country.country, epi_country.epi_regions, epi_country.geo_subregion, epi_country.envhealth, epi_country.high_population_density, epi_country.populationdensity07, epi_country.population07, epi_country.water_h, epi_country.air_h, epi_country.biodiversity)
rows_epi = results_epi.all()
epi_df = pd.DataFrame(rows_epi)
engine.dispose()