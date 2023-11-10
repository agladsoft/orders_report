import re
import os
import sys
import json
import contextlib
import numpy as np
import pandas as pd
from typing import Optional
from pandas import DataFrame
from datetime import datetime

headers_eng: dict = {
    "Дата отхода с/з": "date",
    "Экспедитор": "expeditor",
    "Инд.": "individual",
    "№ конт.": "container",
    "Тип": "container_type_and_size",
    "Груз": "goods_name",
    "Порт назначения": "tracking_seaport",
    "Судно": "ship_name",
    "Линия": "line"
}


DATE_FORMATS: tuple = ("%Y-%m-%d %H:%M:%S", "%d.%m.%Y", "%d.%m.%Y %H:%M:%S", "%d.%m.%Y %H:%M")


class OrdersReport(object):
    def __init__(self, input_file_path: str, output_folder: str):
        self.input_file_path: str = input_file_path
        self.output_folder: str = output_folder

    @staticmethod
    def convert_format_date(date: str) -> Optional[str]:
        """
        Convert to a date type.
        """
        for date_format in DATE_FORMATS:
            with contextlib.suppress(ValueError):
                return str(datetime.strptime(date, date_format).date())
        return None

    def change_type_and_values(self, df: DataFrame) -> None:
        """
        Change data types or changing values.
        """
        with contextlib.suppress(Exception):
            df['date'] = df['date'].apply(lambda x: self.convert_format_date(str(x)))
            df['container_number'] = df['individual'] + df['container']
            df[['container_type', 'container_size']] = df['container_type_and_size'].str.split(expand=True)

    def add_new_columns(self, df: DataFrame) -> None:
        """
        Add new columns.
        """
        df['original_file_name'] = os.path.basename(self.input_file_path)
        df['original_file_parsed_on'] = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    def check_date_in_begin_file(self) -> str:
        """
        Check the date at the beginning of the file.
        """
        date_previous: re.Match = re.match(r'\d{2,4}.\d{1,2}', os.path.basename(self.input_file_path))
        date_previous: str = f'{date_previous.group()}.01' if date_previous else date_previous
        if date_previous is None:
            raise AssertionError('Date not in file name!')
        else:
            return str(datetime.strptime(date_previous, "%Y.%m.%d").date())

    def write_to_json(self, parsed_data: list) -> None:
        """
        Write data to json.
        """
        basename: str = os.path.basename(self.input_file_path)
        output_file_path: str = os.path.join(self.output_folder, f'{basename}.json')
        with open(f"{output_file_path}", 'w', encoding='utf-8') as f:
            json.dump(parsed_data, f, ensure_ascii=False, indent=4)

    def main(self) -> None:
        """
        The main function where we read the Excel file and write the file to json.
        """
        df: DataFrame = pd.read_excel(self.input_file_path, skiprows=1)
        df = df.dropna(axis=0, how='all')
        df = df.rename(columns=headers_eng)
        df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
        self.add_new_columns(df)
        self.change_type_and_values(df)
        df = df.replace({np.nan: None, "NaT": None})
        self.write_to_json(df.to_dict('records'))


if __name__ == "__main__":
    orders_report: OrdersReport = OrdersReport(sys.argv[1], sys.argv[2])
    orders_report.main()
