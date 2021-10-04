from flask import Flask, redirect, url_for, render_template, request
from request_handler import RequestText
from data_handler import DataHandler
from SQL_handler import WeatherSQLlite
import pandas as pd

app = Flask(__name__)

@app.route("/", methods=['POST', 'GET'])
def home():
    if request.method == 'POST':
        data = request.form
        if len(data) and 'question1' in data and len(data['question1']):
            q_1cty = request.form['question1']
            return redirect(url_for("question1", q_1cty=q_1cty))
        elif len(data) and 'question2' in data and len(data['question2']):
            q_2cty = request.form['question2']
            a = url_for("question2", q_2cty=q_2cty)
            return redirect(a)
        else:
            q_3cty = request.form['question3a']
            q3_metric = request.form['metrics']
            q3_num_of_locations = request.form['question3b']
            return redirect(url_for("question3", q_3cty=q_3cty, q3_metric=q3_metric, q3_num_of_locations=q3_num_of_locations))
    else:
        return render_template("base.html")

@app.route("/question1/<q_1cty>", methods=['POST', 'GET'])
def question1(q_1cty):
    cities = q_1cty.split(',')
    results = {}
    for city in cities:
        city_request = RequestText(city)
        city_request.get_forecasts()
        results.update(city_request.results)
    data_handler = DataHandler(results)
    latest_forecasts = data_handler.latest_forecast()
    list_of_tuples = []
    for city, value1 in latest_forecasts.items():
        for date, weather in value1.items():
            temp = [city, date]
            for key, value3 in weather.items():
                temp.append(value3)
            list_of_tuples.append(tuple(temp))
    column_names = ['CITY', 'DATE', 'id', 'weather_state_name', 'weather_state_abbr',
                    'wind_direction_compass','created', 'applicable_date', 'min_temp',
                    'max_temp', 'the_temp', 'wind_speed', 'wind_direction', 'air_pressure',
                    'humidity', 'visibility', 'predictability']
    df = data_handler.convert_to_dataframe(list_of_tuples, column_names)
    return render_template("question1.html", data=df, columns=column_names)

@app.route("/question2/<q_2cty>", methods=['POST', 'GET'])
def question2(q_2cty):
    cities = q_2cty.split(',')
    results = {}
    for city in cities:
        city_request = RequestText(city)
        city_request.get_forecasts()
        results.update(city_request.results)
    data_handler = DataHandler(results)
    avg_temperature = data_handler.avg_temperature()
    df = pd.DataFrame.from_dict(avg_temperature, orient='index')
    df.reset_index(inplace=True)
    df.rename(columns={'index': 'CITY'}, inplace=True)
    column_names = list(df.columns)
    return render_template("question2.html", data=df, columns=column_names)
    
@app.route("/question3/<q_3cty>/<q3_metric>/<q3_num_of_locations>", methods=['POST', 'GET'])
def question3(q_3cty, q3_metric, q3_num_of_locations):
    cities = q_3cty.split(',')
    results = {}
    for city in cities:
        city_request = RequestText(city)
        city_request.get_forecasts()
        results.update(city_request.results)
    data_handler = DataHandler(results)
    columns_name_type = data_handler.get_columns_name_type()
    column_names = list(columns_name_type.keys())
    sql_data = data_handler.get_columns_entry()
    sql = WeatherSQLlite('MyDB.db')
    conn = sql.create_connection(columns_name_type)
    sql.insert_entries_to_DB(sql_data)
    sql_query = 'SELECT CITY, MAX({}) FROM WEATHER GROUP BY CITY ORDER BY {} DESC LIMIT {};'.format(q3_metric, q3_metric, q3_num_of_locations)
    data_stored = sql.get_data(sql_query)
    column_names = ['CITY', q3_metric]
    df = data_handler.convert_to_dataframe(data_stored, column_names)
    return render_template("question3.html", data=df, columns=column_names)    
      
if __name__ == "__main__":
    app.run()

