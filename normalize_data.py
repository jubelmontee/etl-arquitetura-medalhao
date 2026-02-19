import os
import pandas as pd

class FileHandler:
    def __init__(self, input_dir, output_dir):
        self.input_dir = input_dir
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def load_file(self, file):
        input_path = os.path.join(self.input_dir, file)
        name, ext = os.path.splitext(file)

        if ext.lower() == ".csv":
            return pd.read_csv(input_path)
        elif ext.lower() == ".json":
            try:
                return pd.read_json(input_path)
            except ValueError:
                return pd.read_json(input_path, lines=True)
        else:
            print(f"Arquivo {file} ignorado, seu formato não é suportado")
            return None

    def save_file(self, df, file):
        output_path = os.path.join(self.output_dir, f"{os.path.splitext(file)[0]}.parquet")
        df.to_parquet(output_path, index=False)
        print(f"Arquivo {file} normalizado e salvo como {output_path}")

class DataNormalizer:
    @staticmethod
    def convert_columns_to_string(df):
        for col in df.columns:
            if df[col].apply(lambda x: isinstance(x, list)).any():
                df[col] = df[col].apply(lambda x: str(x) if isinstance(x, list) else x)
        return df

    @staticmethod
    def remove_duplicates(df):
        return df.drop_duplicates().reset_index(drop=True)

class NormalizeDataPipeline:
    def __init__(self, input_dir, output_dir):
        self.file_handler = FileHandler(input_dir, output_dir)
        self.normalizer = DataNormalizer()

    def normalize(self):
        # listdir: retorna uma lista contendo os nomes dos arquivos e diretórios na pasta especificada
        for file in os.listdir(self.file_handler.input_dir):
            df = self.file_handler.load_file(file)
            if df is not None:
                df = self.normalizer.convert_columns_to_string(df)
                df = self.normalizer.remove_duplicates(df)
                self.file_handler.save_file(df, file)

if __name__ == "__main__":
    pipeline = NormalizeDataPipeline("bronze", "silver")
    pipeline.normalize()