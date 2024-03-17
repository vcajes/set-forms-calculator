import os
import argparse
import logging
import pathlib
import numpy as np
import pandas as pd
import warnings

logging.basicConfig(format='%(levelname)s %(message)s', level=logging.DEBUG)
warnings.simplefilter("ignore")

COL_IMPUTA_IRP = "Imputa IRP"
COL_NO_IMPUTAR = "No Imputar"
COL_TIPO_REGISTRO = "Tipo de Registro"
COL_MONTO_10 = "Monto Gravado 10%"
COL_MONTO_5 = "Monto Gravado 5%"
COL_MONTO_0 = "Monto No Gravado / Exento "
COL_TOTAL_COMPROBANTE = "Total Comprobante"
COL_TIPO_COMPROBANTE = "Tipo de Comprobante"
COL_RUC = "RUC / N? de Identificacion del Informado"
COL_RUC_EGRESOS = "RUC / N° de Identificación del Informado"
COL_TIPO_TODOS = ["VENTAS", "COMPRAS", "INGRESOS", "EGRESOS"]
TIPO_COMPROBANTE_EXTRACTO_TC = "EXTRACTO DE CUENTA TC/TD"
TIPO_COMPROBANTE_EGRESO_CREDITO = "COMPROBANTE DE EGRESOS POR COMPRAS A CRÉDITO"
TIPO_COMPROBANTE_EGRESO_ENT_PUBLICAS = "COMPROBANTE DE INGRESOS ENTIDADES PÚBLICAS, RELIGIOSA O DE BENEFICIO PÚBLICO"
COL_TIPO_COMPROBANTE_TODOS = [TIPO_COMPROBANTE_EXTRACTO_TC, TIPO_COMPROBANTE_EGRESO_CREDITO, TIPO_COMPROBANTE_EGRESO_ENT_PUBLICAS]


def load_data(path):
    df_compras, df_ventas, df_egresos, df_ingresos = [], [], [], []

    total_loaded = 0
    for xls_file_path in pathlib.Path(path).glob("*.xlsx"):
        if os.path.basename(xls_file_path).startswith("~$"):
            continue  # ignore xlsx file metadata

        total_loaded += 1
        logging.info(f"Loading {xls_file_path}")

        data = pd.read_excel(pd.ExcelFile(xls_file_path), sheet_name="Datos", engine="openpyxl")

        data_compras = data[~data[COL_TIPO_REGISTRO].astype(str).isin([c for c in COL_TIPO_TODOS if c != "COMPRAS"])]
        data_ventas = data[~data[COL_TIPO_REGISTRO].astype(str).isin([c for c in COL_TIPO_TODOS if c != "VENTAS"])]
        data_egresos = data[~data[COL_TIPO_REGISTRO].astype(str).isin([c for c in COL_TIPO_TODOS if c != "EGRESOS"])]
        data_ingresos = data[~data[COL_TIPO_REGISTRO].astype(str).isin([c for c in COL_TIPO_TODOS if c != "INGRESOS"])]

        df_compras.append(data_compras)
        df_ventas.append(data_ventas)
        df_egresos.append(data_egresos)
        df_ingresos.append(data_ingresos)

    compras = pd.concat(df_compras)
    ventas = pd.concat(df_ventas)
    egresos = pd.concat(df_egresos)
    ingresos = pd.concat(df_ingresos)

    logging.info(f"Loaded {total_loaded} total documents")

    return compras, ventas, egresos, ingresos


def clean_compras(compras):
    # keep only columns we need
    compras = compras[[COL_TIPO_REGISTRO, COL_IMPUTA_IRP, COL_RUC, COL_MONTO_10, COL_MONTO_5, COL_MONTO_0]]

    # ensure that the rows are either for IRP deduction or not
    if {COL_IMPUTA_IRP, COL_NO_IMPUTAR}.issubset(compras.columns):
        assert np.where((compras[COL_IMPUTA_IRP] == "NO") & (compras[COL_NO_IMPUTAR] == "NO"))[0].size == 0
        assert np.where((compras[COL_IMPUTA_IRP] == "SI") & (compras[COL_NO_IMPUTAR] == "SI"))[0].size == 0

    # remove rows that will not be deducted for IRP
    compras = compras[~compras[COL_IMPUTA_IRP].astype(str).isin(["NO"])]

    compras[COL_RUC] = compras[COL_RUC].astype(str).apply(lambda x: x.replace(".0", ""))

    # convert amounts to int
    compras[COL_MONTO_10] = compras[COL_MONTO_10].fillna(0).astype(int)
    compras[COL_MONTO_5] = compras[COL_MONTO_5].fillna(0).astype(int)
    compras[COL_MONTO_0] = compras[COL_MONTO_0].fillna(0).astype(int)

    return compras


def clean_ventas(ventas):
    # keep only columns we need
    ventas = ventas[[COL_TIPO_REGISTRO, COL_RUC, COL_MONTO_10, COL_MONTO_5, COL_MONTO_0]]

    ventas[COL_RUC] = ventas[COL_RUC].astype(str).apply(lambda x: x.replace(".0", ""))

    # convert amounts to int
    ventas[COL_MONTO_10] = ventas[COL_MONTO_10].fillna(0).astype(int)
    ventas[COL_MONTO_5] = ventas[COL_MONTO_5].fillna(0).astype(int)
    ventas[COL_MONTO_0] = ventas[COL_MONTO_0].fillna(0).astype(int)

    return ventas


def clean_egresos(egresos):
    egresos = egresos[~egresos[COL_TIPO_COMPROBANTE].astype(str).isin([TIPO_COMPROBANTE_EGRESO_CREDITO])]

    # ensure restrictions are met
    assert np.where((egresos[COL_TIPO_COMPROBANTE] == TIPO_COMPROBANTE_EGRESO_CREDITO))[0].size == 0

    # fix when there is no egresos
    if COL_RUC_EGRESOS not in egresos:
        egresos[COL_RUC_EGRESOS] = "0"

    # keep only columns we need
    egresos = egresos[[COL_TIPO_REGISTRO, COL_RUC_EGRESOS, COL_TIPO_COMPROBANTE, COL_TOTAL_COMPROBANTE]]

    egresos[COL_RUC_EGRESOS] = egresos[COL_RUC_EGRESOS].astype(str).apply(lambda x: x.replace(".0", ""))

    # convert total amount to int
    egresos[COL_TOTAL_COMPROBANTE] = egresos[COL_TOTAL_COMPROBANTE].fillna(0).astype(int)

    return egresos


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', type=str, default="data")
    args = parser.parse_args()

    compras, ventas, egresos, ingresos = load_data(args.path)

    ventas = clean_ventas(ventas)
    compras = clean_compras(compras)
    egresos = clean_egresos(egresos)

    total_compras_10 = compras[COL_MONTO_10].astype(int).sum()
    total_compras_5 = compras[COL_MONTO_5].astype(int).sum()
    total_compras_0 = compras[COL_MONTO_0].astype(int).sum()

    total_ventas_10 = ventas[COL_MONTO_10].astype(int).sum()
    total_ventas_5 = ventas[COL_MONTO_5].astype(int).sum()
    total_ventas_0 = ventas[COL_MONTO_0].astype(int).sum()

    total_egresos = egresos[COL_TOTAL_COMPROBANTE].astype(int).sum()

    total_gastos_0 = total_compras_0 + total_egresos
    total_gastos_5 = total_compras_5
    total_gastos_10 = total_compras_10

    logging.info("")

    logging.info(f"Total compras 10%: {total_compras_10} Gs")
    logging.info(f"Total compras 5%: {total_compras_5} Gs")
    logging.info(f"Total compras 0%: {total_compras_0} Gs")

    logging.info("")

    logging.info(f"Total egresos 0%: {total_egresos} Gs")

    logging.info("")

    logging.info(f"Total ventas 10%: {total_ventas_10} Gs")
    logging.info(f"Total ventas 5%: {total_ventas_5} Gs")
    logging.info(f"Total ventas 0%: {total_ventas_0} Gs")

    logging.info("")

    logging.info(f"Total gastos 10%: {total_gastos_10} Gs")
    logging.info(f"Total gastos 5%: {total_gastos_5} Gs")
    logging.info(f"Total gastos 0%: {total_gastos_0} Gs")

    logging.info("")

    logging.info(f"Successfully generated IVA Form values")
