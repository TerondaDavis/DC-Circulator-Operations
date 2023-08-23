import smartsheet
import pandas as pd
import numpy as np

class SmartsheetManager:
    def __init__(self, api_token):
        self.smartsheet_client = smartsheet.Smartsheet(api_token)

    def import_smartsheet_to_dataframe(self, sheet_id):
        smartsheet_client = self.smartsheet_client
        sheet = smartsheet_client.Sheets.get_sheet(sheet_id)
        if hasattr(sheet, 'message'): 
            print(f"Error fetching sheet {sheet_id}: {sheet.message}")
            return None
        columns = []
        for column in sheet.columns:
            columns.append(column.title)
        data = []
        for row in sheet.rows:
            row_data = []
            for cell in row.cells:
                row_data.append(cell.value)
            data.append(row_data)
        df = pd.DataFrame(data, columns=columns)
        return df

    def fetch_data(self, sheet_id):
        result = self.import_smartsheet_to_dataframe(sheet_id) # Use the correct method name here
        if result is None:
            print(f"Failed to fetch data for sheet {sheet_id}")
            return None
        return result
    
    def export_dataframe_to_smartsheet(self, df, sheet_id):
        smartsheet_client = self.smartsheet_client
        sheet = smartsheet_client.Sheets.get_sheet(sheet_id)
        existing_columns = {col.title: (col.id, col.type) for col in sheet.columns}
        row_ids_to_delete = [row.id for row in sheet.rows]
        chunk_size = 100  
        for i in range(0, len(row_ids_to_delete), chunk_size):
            smartsheet_client.Sheets.delete_rows(sheet_id, row_ids_to_delete[i:i+chunk_size])
        new_columns = []
        for idx, col in enumerate(df.columns):
            if col not in existing_columns:
                new_column = smartsheet.models.Column({
                    'title': col,
                    'primary': False,
                    'type': 'TEXT_NUMBER',
                    'index': idx
                })
                new_columns.append(new_column)
        if new_columns:
            smartsheet_client.Sheets.add_columns(sheet_id, new_columns)
            sheet = smartsheet_client.Sheets.get_sheet(sheet_id)
            existing_columns = {col.title: (col.id, col.type) for col in sheet.columns}
        rows = []
        for index, row_data in df.iterrows():
            row = smartsheet.models.Row()
            row.to_top = True
            cells = []
            for col in df.columns:
                if col in existing_columns:
                    column_id, column_type = existing_columns[col]
                    cell = smartsheet.models.Cell()
                    cell.column_id = column_id
                    cell_value = row_data[col]
                    if isinstance(cell_value, float) and (np.isnan(cell_value) or np.isinf(cell_value)):
                        cell_value = ""
                    if cell_value == "":
                        print(f"Empty value encountered in column {col} at index {index}")
                    cell.value = cell_value
                    cells.append(cell)
            row.cells = cells
            rows.append(row)
        response = smartsheet_client.Sheets.add_rows(sheet_id, rows)
        if response.message != 'SUCCESS':
            print(f'Failed to add rows to sheet: {sheet_id}, Error: {response.message}')
            print(response)
        else:
            print(f'Added {len(response.result)} rows to sheet: {sheet_id}')