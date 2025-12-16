import locale
from io import BytesIO
from datetime import datetime

import pandas as pd
from aws_lambda_powertools import Logger

logger = Logger(child=True)

pd.options.mode.chained_assignment = None
locale.setlocale(locale.LC_TIME, "pt_BR.UTF-8")

palette = [
    "#05132A", "#08306B", "#08519C", "#2171B5", "#4292C6",
    "#6BAED6", "#7FBCD6", "#97CADF", "#B0D7E8", "#C9E4F1"
]

common_x_axis = {
    "major_gridlines": {
        "visible": False
    },
    "line": {
        "color": "#D9D9D9"
    },
    "num_font": {
        "size": 8
    }
}

common_y_axis = {
    "major_gridlines": {
        "visible": True,
        "line": {
            "color": "#D9D9D9"
        }
    },
    "line": {
        "color": "#D9D9D9"
    }
}

class ExcelBuilder:
    
    def __init__(self, logo):
        self.buffer = BytesIO()
        self.writer = pd.ExcelWriter(self.buffer, engine="xlsxwriter")
        self.workbook = self.writer.book
        self.logo = logo

    @property
    def formats(self):
        header_format = self.workbook.add_format({
            "bold": True,
            "font_color": "#05132A",
            "font_size": 14,
            "align": "center",
            "valign": "vcenter"
        })

        subheader_format = self.workbook.add_format({
            "bold": True,
            "font_color": "#808080",
            "font_size": 11,
            "align": "center",
            "valign": "vcenter"
        })

        section_format = self.workbook.add_format({
            "bg_color": "#05132A",
            "font_color": "white",
            "font_size": 28,
            "align": "center",
            "valign": "vcenter"
        })

        bold_format = self.workbook.add_format({"bold": True})
        cell_format = self.workbook.add_format({"text_wrap": True})
        center_cell_format = self.workbook.add_format({"align": "center", "num_format": "0.00"})

        green_cell_format = self.workbook.add_format({
            "align": "center",
            "font_color": "#68BA368",
            "num_format": "0.00"
        })

        red_cell_format = self.workbook.add_format({
            "align": "center",
            "font_color": "#C97C7C",
            "num_format": "0.00"
        })

        return {
            "header": header_format,
            "subheader": subheader_format,
            "bold": bold_format,
            "cell": cell_format,
            "center_cell": center_cell_format,
            "green_cell": green_cell_format,
            "red_cell": red_cell_format,
            "section": section_format
        }

    def add_header(self, worksheet, team):
        merge_range = "A1:AC4"
        worksheet.merge_range(merge_range, f"Análise de custos - {team}", self.formats["section"])
        
        worksheet.insert_image("A2", "btg-white.png",
            {
                "image_data": self.logo,
                "x_scale": 0.03,
                "y_scale": 0.03,
                "x_offset": 30,
                "y_offset": -8
            }
        )

    def format_sheets(self, sheets, team):
        for sheet_name, worksheet in self.writer.book.sheetnames.items():
            if sheet_name not in sheets:
                worksheet.hide()
            else:
                self.add_header(worksheet, team=team)
                worksheet.hide_gridlines(2)
        return

    def worksheet(self, sheet_name):
        try:
            worksheet = self.writer.sheets[sheet_name]
        except KeyError:
            worksheet = self.workbook.add_worksheet(sheet_name)
        return worksheet

    @staticmethod
    def letter(actual_letter, progress):
        num = 0
        for char in actual_letter:
            num = num * 26 + (ord(char) - ord("A") + 1)
        num += progress

        result = ""
        while num > 0:
            num -= 1
            result = chr(num % 26 + ord("A")) + result
            num //= 26
        return result

    def build_stacked_column_chart(self, data, sheet_name, sheet_ref, position):
        num_rows, num_cols = data.shape
        worksheet = self.worksheet(sheet_name)
        chart = self.workbook.add_chart({"type": "column", "subtype": "stacked"})

        for col_num in range(0, num_cols - 1):
            chart.add_series({
                "name": [sheet_ref, 0, col_num],
                "categories": [sheet_ref, 1, num_cols - 1, num_rows, num_cols - 1],
                "values": [sheet_ref, 1, col_num, num_rows, col_num],
                "fill": {"color": palette[col_num]},
                "gap": 50,
            })

        chart.set_title({"name": "TOP 10 Serviços (Mensal)"})
        chart.set_size({"width": 1240, "height": 400})
        chart.set_y_axis(common_y_axis)
        chart.set_x_axis(common_x_axis)
        worksheet.insert_chart(position, chart)
        return

    def build_line_chart(self, data, sheet_name, sheet_ref, position="B24"):
        num_rows, num_cols = data.shape
        worksheet = self.worksheet(sheet_name)
        chart = self.workbook.add_chart({"type": "line"})

        for col_num in range(0, num_cols - 1):
            chart.add_series({
                "name": [sheet_ref, 0, col_num],
                "categories": [sheet_ref, 1, num_cols - 1, num_rows, num_cols - 1],
                "values": [sheet_ref, 1, col_num, num_rows, col_num],
                "line": {"color": palette[col_num]},
            })

        chart.set_title({"name": "TOP 10 Recursos (Diário)"})
        chart.set_size({"width": 825, "height": 400})
        chart.set_y_axis(common_y_axis)
        chart.set_x_axis({**common_x_axis, "num_font": {"size": 10}})
        worksheet.insert_chart(position, chart)
        return

    def build_single_line_chart(self, data, sheet_name, sheet_ref, position, col):
        num_rows, num_cols = data.shape
        worksheet = self.worksheet(sheet_name)
        chart = self.workbook.add_chart({"type": "line"})

        chart.add_series({
            "name": [sheet_ref, 0, col],
            "categories": [sheet_ref, 1, num_cols - 1, num_rows, num_cols - 1],
            "values": [sheet_ref, 1, col, num_rows, col],
            "line": {"color": palette[col]},
            "data_labels": {
                "value": True,
                "num_format": "0.00",
                "position": "above",
                "font": {
                    "bold": False,
                    "size": 8,
                },
            },
        })

        chart.set_title({"name": "Custo geral (Mensal)"})
        chart.set_size({"width": 825, "height": 400})
        chart.set_y_axis(common_y_axis)
        chart.set_x_axis(common_x_axis)
        worksheet.insert_chart(position, chart)
        return
