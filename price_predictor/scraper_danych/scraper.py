import datetime
import time
import requests
import pandas as pd
import os
from pathlib import Path
from dataclasses import dataclass
import numpy as np


@dataclass
class ScrapeCryptoPrice:
    start_date: datetime.datetime
    end_date: datetime.datetime
    token_id: str
    currency: str
    price_dataframe: pd.DataFrame = None
    api_links: list = None

    def _get_unix_time_interval(self, date_tuple: tuple) -> tuple:
        """Funkcja, która zwraca dolną i górną granicę przedziału czasu w formacie UNIX"""
        start_date, end_date = date_tuple
        start_date_in_unix_format = int(time.mktime(start_date.timetuple()))
        end_date_in_unix_format = int(time.mktime(end_date.timetuple()))
        return (str(start_date_in_unix_format), str(end_date_in_unix_format))

    def _get_unix_timestamps_from_time_period(
        self, start_date: datetime.datetime, end_date: datetime.datetime
    ) -> list:
        """Ta funckja jest odpowiedzialna za utworzenie dat
        w odstępach 90 dniowych w podanym okresie czasu"""
        intervals = divmod(
            (end_date - start_date).total_seconds(), 7776000  # 6912000
        )  # podzielenie okresu czasu na przedziały 90 dniowe
        ninety_days_timestamps = [
            start_date + interval * datetime.timedelta(days=90)
            for interval in range(int(intervals[0]))
        ]  # 90 dniowe okresy czasu w podanym przedziale
        ninety_days_timestamps = [
            (ninety_days_timestamps[i], ninety_days_timestamps[i + 1])
            for i in range(len(ninety_days_timestamps) - 1)
        ]
        ninety_days_unix_timestamps = [
            self._get_unix_time_interval(time_interval)
            for time_interval in ninety_days_timestamps
        ]
        return ninety_days_unix_timestamps

    def _get_api_link(
        self,
        start_unix_timestamp: str,
        end_unix_timestamp: str,
        token_id: str = "ethereum",
        currency: str = "usd",
    ) -> str:
        """Ta funkcja tworzy link, który zostanie wykorzystany do komunikacji z API"""
        return f"https://api.coingecko.com/api/v3/coins/{token_id}/market_chart/range?vs_currency={currency}&from={start_unix_timestamp}&to={end_unix_timestamp}"

    def get_token_prices(self):
        """Ta funckja zwraca ceny wybranej kryptowaluty w określonym okresie czasu"""
        if self.api_links:
            prices = []
            for api_link in self.api_links:
                with requests.Session() as session:
                    adapter = requests.adapters.HTTPAdapter(max_retries=5)
                    session.mount("https://", adapter)
                    json_response = session.get(api_link).json()["prices"]
                    prices.append(json_response)
            return np.concatenate(prices)
        return "Use get api links first to use this method"

    def get_api_links(self):
        """Funkcja pobierająca ceny rynkowe danej kryptowaluty, w danym przedziale czasu"""
        api_links = []
        unix_intervals = self._get_unix_timestamps_from_time_period(
            self.start_date, self.end_date
        )
        for start_unix_timestamp, end_unix_timestamp in unix_intervals:
            api_link = self._get_api_link(
                start_unix_timestamp=start_unix_timestamp,
                end_unix_timestamp=end_unix_timestamp,
                token_id=self.token_id,
                currency=self.currency,
            )
            api_links.append(api_link)
        self.api_links = api_links
        return self


def save_token_historical_data(price_list, token_name):
    price_dataframe = pd.DataFrame(price_list, columns=["Timestamp", "Price"])
    directory_path = Path(__file__).resolve().parents[1]
    saving_path = os.path.join(directory_path, "dane", f"{token_name}_prices.csv")
    price_dataframe.to_csv(saving_path)
    return price_dataframe


if __name__ == "main":
    date_a = datetime.datetime(2017, 1, 1, 00, 1)
    date_b = datetime.datetime(2022, 8, 30, 00, 1)
    api_links = ScrapeCryptoPrice(date_a, date_b, "ethereum", "usd").get_api_links()
    create_ethereum_prices_dataset = save_token_historical_data(
        elko.get_token_prices(), "ethereum"
    )
