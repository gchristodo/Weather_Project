import requests
from datetime import datetime, timedelta


class RequestText:
    def __init__(self, city_name):
        self.city_name = city_name
        self.woeid = None
        self.results = None
        
    def create_url(self, date=None, kind='city'):
        if kind == 'city':
            url = 'https://www.metaweather.com/api/location/search/?query='
            created_url = url + self.city_name
        elif kind == 'woeid' and self.woeid:
            url = 'https://www.metaweather.com/api/location/'
            created_url = url + self.woeid
        elif kind == 'date' and date and self.woeid:
            url = 'https://www.metaweather.com/api/location/'
            created_url = url + self.woeid + '/' + date
        return created_url
    
    def check_status(self, response):
        if response.status_code == 200:
            return True
        return False

    def get_response_json(self, url):
        response = requests.get(url)
        if self.check_status(response):
            return response.json()
        else:
            raise ValueError('Request failed with status error {}'.format(response.status_code))
            
    def execute_query(self, date=None, kind='city'):
        created_url = self.create_url(date, kind=kind)
        return self.get_response_json(created_url)

    def get_woeid(self, json):
        if len(json) == 0:
            raise ValueError('City not found.')
        elif len(json) > 1:
            raise ValueError('Multiple Cities found: {}. Be more specific please.'.format([el['title'] for el in json]))
        else:
            woeid = json[0]['woeid']
            self.woeid = str(woeid)
            return
        
    def create_dates(self):
        date_now = datetime.now()
        dates = []
        for i in range(0, 7):
            future_date = date_now + timedelta(days=i)
            dates.append(future_date.strftime("%Y/%m/%d")) 
        return dates

    def get_forecasts(self):
        dates = self.create_dates()
        city_json = self.execute_query(kind='city')
        city_name_in_json = city_json[0]['title']
        self.get_woeid(city_json)
        results = {}
        results[city_name_in_json] = {}
        for date in dates:
            weather_json = self.execute_query(date, kind='date')
            results[city_name_in_json][date] = weather_json 
        self.results = results
        return
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    