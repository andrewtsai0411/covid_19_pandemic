import sqlite3
import pandas as pd

class CreateCovid19DB:
    def create_time_series(self):
        confirmed = pd.read_csv('data/time_series_covid19_confirmed_global.csv')
        deaths = pd.read_csv('data/time_series_covid19_deaths_global.csv')
        vaccine = pd.read_csv('data/time_series_covid19_vaccine_global.csv')

        # 將confirmed, death利用melt方法轉為長格式
        id_vars = ['Province/State', 'Country/Region', 'Lat', 'Long']
        melted_confirmed = confirmed.melt(id_vars=id_vars, var_name='Date', value_name='Confirmed')
        melted_deaths = deaths.melt(id_vars=id_vars, var_name='Date', value_name='Deaths')

        # 將Date欄位轉換為datetime格式
        melted_confirmed['Date'] = pd.to_datetime(melted_confirmed['Date'], format='%m/%d/%y')
        melted_deaths['Date'] = pd.to_datetime(melted_deaths['Date'], format='%m/%d/%y')

        # 調整vaccine Province_State, Date資料類型 / Province_State, Country_Region重新命名(讓join keys名稱一致)
        vaccine['Province_State'] = vaccine['Province_State'].astype(object)
        vaccine['Date'] = pd.to_datetime(vaccine['Date'])
        vaccine.rename(columns={'Province_State': 'Province/State', 'Country_Region': 'Country/Region'}, inplace=True)

        # 選擇要用的欄位
        melted_confirmed = melted_confirmed.drop(labels=['Lat', 'Long'], axis=1)
        melted_deaths = melted_deaths.drop(labels=['Lat', 'Long'], axis=1)
        vaccine = vaccine.drop(labels=['UID', 'People_at_least_one_dose'], axis=1)

        # join melted_confirmed, melted_deaths, vaccine on 'Province/State', 'Country/Region', 'Date'
        join_keys = ['Province/State', 'Country/Region', 'Date']
        time_series = pd.merge(melted_confirmed, melted_deaths, on=join_keys, how='left')
        time_series = pd.merge(time_series, vaccine, on=join_keys, how='left')

        # time_series調整
        time_series.drop('Province/State',axis=1, inplace= True)
        time_series = time_series.groupby(['Country/Region', 'Date'])[['Confirmed', 'Deaths', 'Doses_admin']].sum().reset_index()
        time_series.columns = ['country', 'reported_on', 'confirmed', 'deaths', 'doses_administered']
        time_series['doses_administered'] = time_series['doses_administered'].astype(int)
        return time_series

    def create_daily_report(self):
        daily_report = pd.read_csv('data/03-09-2023.csv')
        daily_report = daily_report[['Country_Region', 'Province_State', 'Admin2', 'Confirmed', 'Deaths', 'Lat', 'Long_']]
        daily_report.columns = ['country', 'province', 'county', 'confirmed', 'deaths', 'latitude', 'longitude']
        return daily_report

    def create_database(self):
        time_series = self.create_time_series()
        time_series['reported_on'] = time_series['reported_on'].map(lambda x: x.strftime('%Y-%m-%d'))
        daily_report = self.create_daily_report()
        connection = sqlite3.connect('data/covid_19.db')
        time_series.to_sql('time_series', con=connection, if_exists='replace', index= False)
        daily_report.to_sql('daily_report', con=connection, if_exists='replace', index= False)
        connection.close()

create_covid_19_DB = CreateCovid19DB()
create_covid_19_DB.create_database()