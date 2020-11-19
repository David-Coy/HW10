import datetime as dt
import numpy as np
import pandas as pd

import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, desc

from flask import Flask, jsonify
import pdb

#################################################
# Database Setup
#################################################
engine = create_engine("sqlite:///resources/hawaii.sqlite")

# reflect an existing database into a new model
Base = automap_base()# Declare a Base using `automap_base()`
# reflect the tables
Base.prepare(engine, reflect=True)# Use the Base class to reflect the database tables

# Save references to each table
Measurement = Base.classes.measurement # Assign the measurment class to a variable called `Measurement`
Station = Base.classes.station

# Create our session (link) from Python to the DB
session = Session(engine)

#################################################
# Flask Setup
#################################################
app = Flask(__name__)


#################################################
# Flask Routes
#################################################

#last_date = session.query(Measurement.date).order_by(desc(Measurement.date)).first()[0]
#prev_year = dt.datetime.strptime(last_date, '%Y-%m-%d') - dt.timedelta(days=365)


@app.route("/")
def welcome():
    return (
        f"Welcome to the HOME PAGE!<br/>"
        f"Welcome to the Hawaii Climate Analysis API!<br/>"
        f"<br/>"
        f"Available Routes:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"/api/v1.0/stations<br/>"
        f"/api/v1.0/tobs<br/>"
        f"/api/v1.0/temp/start/end"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    """Return the precipitation data for the last year"""
    # Calculate the date 1 year ago from last date in database
    last_date = session.query(Measurement.date).order_by(desc(Measurement.date)).first()[0]
    prev_year = dt.datetime.strptime(last_date, '%Y-%m-%d') - dt.timedelta(days=365)

    # Query for the date and precipitation for the last year
    precipitation = session.query(Measurement.date, Measurement.prcp).filter(Measurement.date >= prev_year).all()

    # Dict with date as the key and prcp as the value
    precip = {date: prcp for date, prcp in precipitation}#Use dictionary comprehension!
    return jsonify(precip)



@app.route("/api/v1.0/stations")# route binds a function to a url
def stations():
    "Return a JSON list of stations from the dataset."
    results = session.query(Station.station).all()
    #stations = [results[0] for result in results] #This leaves a comma in the data and thus JSON formatting gets weird
    stations = list(np.ravel(results))# This leave a cleaner result tha above
    return jsonify(STATIONS=stations)


@app.route("/api/v1.0/tobs")
def tobs():
    "Return a JSON list of temperature observations (TOBS) for the previous year"
    
    prev_year = dt.date(2017, 8, 23) - dt.timedelta(days=365)
    
    most_active_stations = session.query(Measurement.station, func.count(Measurement.station)).group_by(Measurement.station).order_by(func.count(Measurement.station).desc()).all()
    most_active_station = most_active_stations[0][0]
    
    results = session.query(Measurement.tobs).filter(Measurement.station == most_active_station).filter(Measurement.date >= prev_year).all()

    # Unravel results into a 1D array and convert to a list
    temps = list(np.ravel(results))

    # Return the results
    return jsonify(Temperatures=temps)


@app.route("/api/v1.0/temp/<start>")
@app.route("/api/v1.0/temp/<start>/<end>") #https://flask.palletsprojects.com/en/1.1.x/quickstart/
def stats(start=None, end=None):
    """Return TMIN, TAVG, TMAX."""

    # Select statement
    sel = [func.min(Measurement.tobs), func.avg(Measurement.tobs), func.max(Measurement.tobs)]
    #pdb.set_trace()
    if not end:
        # calculate TMIN, TAVG, TMAX for dates greater than start
        results = session.query(*sel).filter(Measurement.date >= start).all()
        # Unravel results into a 1D array and convert to a list
        temps = list(np.ravel(results))
         
        return jsonify(temps=temps)

    # calculate TMIN, TAVG, TMAX with start and stop
    results = session.query(*sel).\
        filter(Measurement.date >= start).\
        filter(Measurement.date <= end).all()
    # Unravel results into a 1D array and convert to a list
    temps = list(np.ravel(results))
    return jsonify(temps=temps)




if __name__ == '__main__':
    app.run()
