import fitz
import pandas as pd
import re
import uuid
from sqlalchemy import create_engine

PDF_LIST = [
    "assets/pdf/ControleODONTO.pdf",
    "assets/pdf/ControleODONTO01-01-2024.pdf",
    "assets/pdf/ControleODONTO02-06-2024.pdf"
]

DB_USER = ""
DB_PASS = ""
DB_HOST = ""
DB_NAME = ""

REGEX_PATTERNS = {
    "data": r"^\d{2}/\d{2}/\d{4}$",
    "hora_inicio_fim": r"^\d{2}:\d{2} - \d{2}:\d{2}$",
    "paciente": r"^.+ - \d{6}$",
    "telefone": r"^\(\d{2}\)\d{4,5}-\d{4}$",
    "tipo_atendimento": r"^(Avaliação|Retorno|Consulta|Compromisso).*",
    "observações": r"^(?!Alessandra$).{1,100}$",
    "profissional": r"^Alessandra$",
    "data_cadastro": r"^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$",
    "alteracao": r"^Alterado em \d{2}/\d{2}/\d{4}$",
    "hora_alteracao": r"^\d{2}:\d{2}$"
}

dataframe_dict = {
    "data": [],
    "hora_inicio_fim": [],
    "paciente": [],
    "telefone": [],
    "tipo_atendimento": [],
    "observações": [],
    "profissional": [],
    "data_cadastro": [],
    "alteracao": [],
    "hora_alteracao": []
}


def get_data_list(doc) -> list:
    lista = []

    for page in doc:
        list_page = page.get_text("blocks")

        for item in list_page:
            valor = item[4]

            if ("Floriano Peixoto, 323" in valor
                    or "CEP" in valor
                    or "Fone" in valor
                    or "AGENDAMENTOS" in valor
                    or "Convênio" in valor
                    or "http" in valor
                    or "ControleODONTO" in valor
            ):
                continue

            for data in valor.split("\n"):
                if data != '':
                    lista.append(data.strip())

    return lista


def get_dict(lista: list) -> dict:
    for i in range(0, len(lista), 10):
        bloco = lista[i:i + 10]
        registro = {
            "data": "",
            "hora_inicio_fim": "",
            "paciente": "",
            "telefone": "",
            "tipo_atendimento": "",
            "observações": "",
            "profissional": "",
            "data_cadastro": "",
            "alteracao": "",
            "hora_alteracao": ""
        }
        for item in bloco:
            for campo, padrao in REGEX_PATTERNS.items():
                if registro[campo] == "" and re.match(padrao, item):
                    registro[campo] = item
                    break

        # Preencher o dicionário final
        for campo in dataframe_dict:
            dataframe_dict[campo].append(registro[campo])

    return dataframe_dict


def write_consultation_type(df: pd.DataFrame, engine) -> pd.Series:
    regex_list = [r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$', r'^\d{2}:\d{2}$', r"\(\d{2}\)9\d{4}-\d{4}", r".*Alterado.*"]

    for regex in regex_list:
        df = df[~df['observações'].str.contains(regex, na=False)]

    df = df.rename(columns={'observações': 'label'})
    df['excluded'] = False
    df['id'] = df['label'].apply(lambda x: uuid.uuid5(uuid.NAMESPACE_DNS, x))

    df_consultation_types = df[["id", "label", "excluded"]]
    df_consultation_types = df_consultation_types.drop_duplicates(subset=['id'], keep='first')

    #df_consultation_types.to_sql("consultation_types", engine, if_exists="append", index=False, schema="dente_de_leao_manager")

    df["concluded"] = True
    df = df.rename(columns={"paciente": "patient_name"})
    df["consultation_type_id"] = df['id']

    df["start_date"] = df["data"] + " " + df["hora_inicio_fim"].str.split(" - ").str[0]
    df["start_date"] = df["start_date"].str.replace("/", "-")
    df["start_date"] = pd.to_datetime(df["start_date"], format="%d-%m-%Y %H:%M")

    df["end_date"] = df["data"] + " " + df["hora_inicio_fim"].str.split(" - ").str[1]
    df["end_date"] = df["end_date"].str.replace("/", "-")
    df["end_date"] = pd.to_datetime(df["end_date"], format="%d-%m-%Y %H:%M")

    df["id"] = df.apply(lambda row: uuid.uuid5(uuid.NAMESPACE_DNS, f"{row['patient_name']}{row['start_date']}"), axis=1)
    df_consultations = df[["id", "patient_name", "start_date", "end_date", "concluded", "consultation_type_id"]]
    df_consultations.to_sql("consultations", engine, if_exists="append", index=False,
                                 schema="dente_de_leao_manager")


def write_consultations(df: pd.DataFrame, engine) -> None:
    df_consultations = df.rename(columns={"paciente": "patient_name"})

    df_consultations["concluded"] = True

    df_consultations["start_date"] = df_consultations["data"] + " " + df_consultations["hora_inicio_fim"].str.split("-").str[0]
    df_consultations["start_date"] = df_consultations["start_date"].str.replace("/", "-")

    df_consultations["end_date"] = df_consultations["data"] + " " + df_consultations["hora_inicio_fim"].str.split("-").str[1]
    df_consultations["end_date"] = df_consultations["end_date"].str.replace("/", "-")

    regex_list = [r'^\d{2}/\d{2}/\d{4} \d{2}:\d{2}$', r'^\d{2}:\d{2}$', r"\(\d{2}\)9\d{4}-\d{4}", r".*Alterado.*"]

    for regex in regex_list:
         df_consultations["consultation_type_id"] = df_consultations[~df_consultations['observações'].str.contains(regex, na=False)]

    df_consultations["id"] = df_consultations.apply(lambda row: uuid.uuid5(uuid.NAMESPACE_DNS, f"{row['patient_name']}{row['start_date']}"), axis=1)
    print(df_consultations)

    df_consultations = df_consultations[["id", "patient_name", "start_date", "end_date", "concluded", "consultation_type_id"]]

    df_consultations.to_sql("consultations", engine, if_exists="append", index=False,
                                 schema="dente_de_leao_manager")


if __name__ == '__main__':
    df_union = pd.DataFrame()
    for pdf in PDF_LIST:
        doc = fitz.open(pdf)

        lista = get_data_list(doc)
        dataframe_dict = get_dict(lista)

        df = pd.DataFrame(dataframe_dict)
        df_union = pd.concat([df_union, df], ignore_index=True)

    engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:5432/{DB_NAME}")

    write_consultation_type(df_union, engine)

    #write_consultations(df, engine)
        #df.to_csv(f"assets/csv/{pdf.split('/')[2].replace('.pdf', '')}.csv", index=False, encoding='utf-8-sig')
