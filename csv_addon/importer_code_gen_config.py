class ImportConfig:
    class FunctionName:
        import_csv = "import_csv"
        @staticmethod
        def row_converter(table_name: str):
            return f"import_{table_name}"

    class VariableName:
        csv_path = "csv_path"
        csv_file = "csv_file"
        csv_row = "csv_row"
        csv_field = "csv_field"
        csv_value = "csv_value"
        entries = "entries"
        @staticmethod
        def table_object(table_name: str):
            return table_name

