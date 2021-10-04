import pandas as pd

class DataHandler:
    def __init__(self, forecast_json):
        self.data = forecast_json
        
    def convert_to_dataframe(self, list_of_tuples, columns):
        df = pd.DataFrame(list_of_tuples, columns=columns)
        return df
        
    def latest_forecast(self):
        results = {}
        for city, dic in self.data.items():
            results[city] = {}
            for date, forecasts in dic.items():
                results[city][date] = forecasts[0]
        return results
         
    def get_columns_name_type(self):
        column_name_types = {}
        column_name_types['CITY'] = 'TEXT'
        column_name_types['DATE'] = 'TEXT'
        for city, value1 in self.data.items():
            for date, value2 in value1.items():
                for weather_json in value2:
                    for key, value3 in weather_json.items():
                        type = ''
                        if isinstance(value3, str):
                            type = 'TEXT'
                        elif isinstance(value3, int):
                            type = 'INTEGER'
                        elif isinstance(value3, float):
                            type = 'REAL'
                        column_name_types[key] = type
                    return column_name_types
             
    def get_columns_entry(self):
        all_rows_data = []
        begin = 'INSERT INTO WEATHER ('
        for city, value1 in self.data.items():
            for date, value2 in value1.items():
                for weather_json in value2:
                    column_names = begin + 'CITY'+ ','+ 'DATE' + ','
                    row_data = [city, date]
                    questionmarks = '?' + ',' + '?' + ','
                    for key, value3 in weather_json.items(): 
                        column_names += key + ','
                        row_data.append(value3)
                        questionmarks += '?' + ','
                    questionmarks = questionmarks[:-1]    
                    final_sql_insert = column_names[:-1] + ') VALUES ('+questionmarks+');'
                    all_rows_data.append((final_sql_insert, tuple(row_data)))   
        return all_rows_data
                            
    def average(self, my_list):
        return round(sum(my_list) / len(my_list), 2)
    
    def avg_temperature(self):
        results = {}
        for city, dic in self.data.items():
            results[city] = {}
            for date, forecasts in dic.items():
                 temp_1 = forecasts[0]['the_temp']
                 temp_2 = forecasts[1]['the_temp']
                 temp_3 = forecasts[2]['the_temp']
                 results[city][date] = self.average([temp_1, temp_2, temp_3])
        return results        
            
            
