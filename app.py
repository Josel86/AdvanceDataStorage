# Importa las dependencias
import datetime as dt
import numpy as np
import pandas as pd
import datetime as dt
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func
from flask import Flask, jsonify

# Se crea la conexión
engine = create_engine("sqlite:///Resources/Hawaii.sqlite")
# Se refleja la base de datos
Base = automap_base()
# Se reflejan las tablas
Base.prepare(engine, reflect=True)

# Se almacena la información en clases
Station = Base.classes.station
Measurements = Base.classes.measurements

# Se crea la sesión con la base de datos
session = Session(engine)

# Se inicializa Flask
app = Flask(__name__)

#Se definene las rutas
@app.route("/")
def welcome():
    return (
        f"Rutas:<br/>"
        f"/api/v1.0/precipitation<br/>"
        f"Muestra los totales de lluvia del año anterior de todas las estaciones<br/>"
        f"/api/v1.0/stations<br/>"
        f"Muestra el numero de estaciones y los nombres<br/>"
        f"/api/v1.0/tobs<br/>"
        f"Muestra las temperaturas del año anterior de todas las estaciones.<br/>"
        f"/api/v1.0/start<br/>"
        f"De acuerdo a la fecha de inicio (YYYY-MM-DD), se calcula la temperatura para todas las fechas mayores e iguales <br/>"
        f"/api/v1.0/start/end<br/>"
        f"De acuerdo a las fechas de inicio y finalización (YYYY-MM-DD), se calcula la temperatura para las fechas entre la fecha de inicio y la de finalización<br/>"
    )

@app.route("/api/v1.0/precipitation")
def precipitation():
    last_date = session.query(Measurements.date).order_by(Measurements.date.desc()).first()
    last_year = dt.date(2017, 8, 23) - dt.timedelta(days=365)
    rain = session.query(Measurements.date, Measurements.prcp).\
        filter(Measurements.date > last_year).\
        order_by(Measurements.date).all()

    rain_totals = []
    for result in rain:
        row = {}
        row["date"] = rain[0]
        row["prcp"] = rain[1]
        rain_totals.append(row)

    return jsonify(rain_totals)

@app.route("/api/v1.0/stations")
def stations():
    stations_query = session.query(Station.name, Station.station)
    stations = pd.read_sql(stations_query.statement, stations_query.session.bind)
    return jsonify(stations.to_dict())

@app.route("/api/v1.0/tobs")
def tobs():
    last_date = session.query(Measurements.date).order_by(Measurements.date.desc()).first()
    last_year = dt.date(2017, 8, 23) - dt.timedelta(days=365)
    temperature = session.query(Measurements.date, Measurements.tobs).\
        filter(Measurements.date > last_year).\
        order_by(Measurements.date).all()

    temperature_totals = []
    for result in temperature:
        row = {}
        row["date"] = temperature[0]
        row["tobs"] = temperature[1]
        temperature_totals.append(row)

    return jsonify(temperature_totals)

@app.route("/api/v1.0/<start>")
def trip1(start):
    start_date= dt.datetime.strptime(start, '%Y-%m-%d')
    last_year = dt.timedelta(days=365)
    start = start_date-last_year
    end =  dt.date(2017, 8, 23)
    trip_data = session.query(func.min(Measurements.tobs), func.avg(Measurements.tobs), func.max(Measurements.tobs)).\
        filter(Measurements.date >= start).filter(Measurements.date <= end).all()
    trip = list(np.ravel(trip_data))
    return jsonify(trip)

@app.route("/api/v1.0/<start>/<end>")
def trip2(start,end):
    start_date= dt.datetime.strptime(start, '%Y-%m-%d')
    end_date= dt.datetime.strptime(end,'%Y-%m-%d')
    last_year = dt.timedelta(days=365)
    start = start_date-last_year
    end = end_date-last_year
    trip_data = session.query(func.min(Measurements.tobs), func.avg(Measurements.tobs), func.max(Measurements.tobs)).\
        filter(Measurements.date >= start).filter(Measurements.date <= end).all()
    trip = list(np.ravel(trip_data))
    return jsonify(trip)

if __name__ == "__main__":
    app.run(debug=True)