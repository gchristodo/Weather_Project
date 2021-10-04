from request_handler import RequestText
from data_handler import DataHandler
from SQL_handler import WeatherSQLlite

   

if __name__ == '__main__':
    cities = ['athens', 'new york', 'london']
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

    latest_forecasts = data_handler.latest_forecast()
    avg_temperature = data_handler.avg_temperature()

    data_stored = sql.get_data('SELECT CITY, MAX(weather_state_name) FROM WEATHER GROUP BY CITY ORDER BY weather_state_name DESC LIMIT 3;')
    df = data_handler.convert_to_dataframe(data_stored, ['CITY', 'the_temp'])
    print(latest_forecasts)
    print(avg_temperature)
    df.to_csv('Results1.csv', index=False)