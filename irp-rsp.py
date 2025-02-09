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
COL_TIMBRADO = "Timbrado del Comprobante"
COL_TOTAL_COMPROBANTE = "Total Comprobante"
COL_NUMERO_COMPROBANTE = "Numero de Comprobante"
COL_RUC = "RUC / Nº de Identificacion del Informado"
COL_RUC_EGRESOS = "RUC / N° de Identificación del Informado"
COL_CONDICION_OPERACION = "Condicion de la Operacion"
COL_TIPO_COMPROBANTE = "Tipo de Comprobante"
COL_TIPO_TODOS = ["VENTAS", "COMPRAS", "INGRESOS", "EGRESOS"]

RUCS_EGRESOS_ACTIV_GRAVADA = [
    "80081262",  # COMPUMARKET S.A.
    "80003128",  # TOYOTOSHI SA
    "80126207",  # TOYOTOSHI GROUP SA
    "80024191",  # ESSAP SA
    "80044227",  # BANCO GNB PARAGUAY SAECA
    "80030572",  # AMX PARAGUAY SA
    "80085098",  # BEBIDAS NATIVAS DEL PARAGUAY SA
    "80033722",  # SERVICIOS MEDICOS MIGONE SOCIEDAD ANONIMA
    "80017437",  # NUCLEO SA
    "80002201",  # BANCO ITAU PARAGUAY S.A
    "80025958",  # PLAZA OFERTA S.A.
    "3626475",   # FIXO CARGO
    "80009735",  # ANDE
    "80016742",  # ESTACION BAHIA SA
    "80040939",  # GESTION DE SERVICIOS SA
    "80019551",  # CADENA FARMACENTER SA
    "80032012",  # GRUPO ENERGY S.A.
    "80030535",  # FARMACIAS CATEDRAL SA
    "80011311",  # DIAZ GILL MEDICINA LABORATORIAL SA
    "80022877",  # FARMA S.A.
    "80023598",  # SANATORIO MIGONE BATTILANA SA
    "80001513",  # NUEVA AMERICANA SA
    "349840",    # JUAN ORLANDO PEREIRA MENDEZ
    "80082790",  # VIGOR SA
    "1238373",   # BERTA
    "80003064",  # WASHINGTON SRL
    "2956920",   # GUSTAVO DANIEL CACERES KALLSEN
    "80004379",  # HERIMARC SRL
    "80088090",  # MASQUELIER MEDICINA INTEGRATIVA SA
    "80004261",  # MICROLIDER
    "80031970",  # TUPI RAMOS GENERALES S.A.
    "80000747",  # COOMECIPAR LTDA.
    "80034461",  # SUDAMERIS BANK SAECA
    "80022557",  # FERIA ASUNCION SA
]

RUCS_ESTADO_ASOCIACIONES = [
    "80004239",  # MOPC
    "80027621",  # CLUB CENTENARIO
    "80029733",  # CLUB OLIMPIA
    "80031086",  # CLUB NAUTICO
]

GASTOS_EXTERIOR_SALUD_EDUCACION = 0

GASTOS_VEHICULOS_CADA_3Y = 0


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


def compras_credito(compras):
    compras = compras[~compras[COL_CONDICION_OPERACION].astype(str).isin(["CONTADO", "Contado"])]
    compras = compras[[COL_TIPO_REGISTRO, COL_RUC, COL_NUMERO_COMPROBANTE, COL_TOTAL_COMPROBANTE]]
    compras[COL_RUC] = compras[COL_RUC].astype(str).apply(lambda x: x.replace(".0", ""))
    compras[COL_TOTAL_COMPROBANTE] = compras[COL_TOTAL_COMPROBANTE].fillna(0).astype(int)
    return compras

def compras_contado(compras):
    compras = compras[~compras[COL_CONDICION_OPERACION].astype(str).isin(["CREDITO", "Crédito"])]
    compras = compras[[COL_TIPO_REGISTRO, COL_RUC, COL_NUMERO_COMPROBANTE, COL_TOTAL_COMPROBANTE]]
    compras[COL_RUC] = compras[COL_RUC].astype(str).apply(lambda x: x.replace(".0", ""))
    compras[COL_TOTAL_COMPROBANTE] = compras[COL_TOTAL_COMPROBANTE].fillna(0).astype(int)
    return compras

def compras_imputa(compras):
    compras = compras[~compras[COL_IMPUTA_IRP].astype(str).isin(["NO"])]
    compras = compras[[COL_TIPO_REGISTRO, COL_RUC, COL_NUMERO_COMPROBANTE, COL_TOTAL_COMPROBANTE]]
    compras[COL_RUC] = compras[COL_RUC].astype(str).apply(lambda x: x.replace(".0", ""))
    compras[COL_TOTAL_COMPROBANTE] = compras[COL_TOTAL_COMPROBANTE].fillna(0).astype(int)
    return compras

def compras_no_imputa(compras):
    compras = compras[~compras[COL_IMPUTA_IRP].astype(str).isin(["SI"])]
    compras = compras[[COL_TIPO_REGISTRO, COL_RUC, COL_NUMERO_COMPROBANTE, COL_TOTAL_COMPROBANTE]]
    compras[COL_RUC] = compras[COL_RUC].astype(str).apply(lambda x: x.replace(".0", ""))
    compras[COL_TOTAL_COMPROBANTE] = compras[COL_TOTAL_COMPROBANTE].fillna(0).astype(int)
    return compras

def clean_compras(compras):
    # remove rows CREDITO as those cannot be deducted without an EGRESOS entry
    compras = compras[~compras[COL_CONDICION_OPERACION].astype(str).isin(["CREDITO", "Crédito"])]

    # ensure that the rows are either for IRP deduction or not
    if {COL_IMPUTA_IRP, COL_NO_IMPUTAR}.issubset(compras.columns):
        assert np.where((compras[COL_IMPUTA_IRP] == "NO") & (compras[COL_NO_IMPUTAR] == "NO"))[0].size == 0
        assert np.where((compras[COL_IMPUTA_IRP] == "SI") & (compras[COL_NO_IMPUTAR] == "SI"))[0].size == 0

    # remove rows that will not be deducted for IRP
    compras = compras[~compras[COL_IMPUTA_IRP].astype(str).isin(["NO"])]

    # ensure restrictions are met
    assert np.where((compras[COL_CONDICION_OPERACION] == "CREDITO"))[0].size == 0
    assert np.where((compras[COL_CONDICION_OPERACION] == "Crédito"))[0].size == 0
    assert np.where((compras[COL_IMPUTA_IRP] == "NO"))[0].size == 0

    # keep only columns we need
    compras = compras[[COL_TIPO_REGISTRO, COL_RUC, COL_NUMERO_COMPROBANTE, COL_TOTAL_COMPROBANTE]]

    compras[COL_RUC] = compras[COL_RUC].astype(str).apply(lambda x: x.replace(".0", ""))

    # convert total amount to int
    compras[COL_TOTAL_COMPROBANTE] = compras[COL_TOTAL_COMPROBANTE].fillna(0).astype(int)

    return compras


def clean_ventas(ventas):
    # remove rows CREDITO as those cannot be added with there is INGRESOS associated
    ventas = ventas[~ventas[COL_CONDICION_OPERACION].astype(str).isin(["CREDITO", "Crédito"])]

    # remove rows that will not be deducted for IRP
    ventas = ventas[~ventas[COL_IMPUTA_IRP].astype(str).isin(["NO"])]

    # skip types that are unrelated to professional services
    ventas = ventas[~ventas[COL_TIPO_COMPROBANTE].astype(str).isin(["NOTA DE CRÉDITO"])]

    # ensure restrictions are met
    assert np.where((ventas[COL_CONDICION_OPERACION] == "CREDITO"))[0].size == 0
    assert np.where((ventas[COL_CONDICION_OPERACION] == "Crédito"))[0].size == 0
    assert np.where((ventas[COL_IMPUTA_IRP] == "NO"))[0].size == 0

    # keep only columns we need
    ventas = ventas[[COL_TIPO_REGISTRO, COL_RUC, COL_NUMERO_COMPROBANTE, COL_TOTAL_COMPROBANTE]]

    ventas[COL_RUC] = ventas[COL_RUC].astype(str).apply(lambda x: x.replace(".0", ""))

    # convert total amount to int
    ventas[COL_TOTAL_COMPROBANTE] = ventas[COL_TOTAL_COMPROBANTE].fillna(0).astype(int)

    return ventas


def clean_egresos(egresos):
    # remove rows that will not be deducted for IRP
    egresos = egresos[~egresos[COL_IMPUTA_IRP].astype(str).isin(["NO"])]

    assert np.where((egresos[COL_IMPUTA_IRP] == "NO"))[0].size == 0

    # keep only columns we need
    egresos = egresos[[COL_TIPO_REGISTRO, COL_RUC_EGRESOS, COL_NUMERO_COMPROBANTE, COL_TOTAL_COMPROBANTE]]

    egresos[COL_RUC_EGRESOS] = egresos[COL_RUC_EGRESOS].astype(str).apply(lambda x: x.replace(".0", ""))

    # convert total amount to int
    egresos[COL_TOTAL_COMPROBANTE] = egresos[COL_TOTAL_COMPROBANTE].fillna(0).astype(int)

    return egresos


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', type=str, default="data")
    args = parser.parse_args()

    compras, ventas, egresos, ingresos = load_data(args.path)

    logging.info("")

    raw_compras = compras[COL_TOTAL_COMPROBANTE].astype(int).sum()
    raw_ventas = ventas[COL_TOTAL_COMPROBANTE].astype(int).sum()
    raw_egresos = egresos[COL_TOTAL_COMPROBANTE].astype(int).sum()

    logging.info(f"RAW compras: {raw_compras:,} Gs")
    logging.info(f"RAW ventas: {raw_ventas:,} Gs")
    logging.info(f"RAW egresos: {raw_egresos:,} Gs")

    logging.info("")

    logging.info(f"RAW compras credito: {compras_credito(compras)[COL_TOTAL_COMPROBANTE].astype(int).sum():,} Gs")
    logging.info(f"RAW compras contado: {compras_contado(compras)[COL_TOTAL_COMPROBANTE].astype(int).sum():,} Gs")
    logging.info(f"RAW compras imputa: {compras_imputa(compras)[COL_TOTAL_COMPROBANTE].astype(int).sum():,} Gs")
    logging.info(f"RAW compras no imputa: {compras_no_imputa(compras)[COL_TOTAL_COMPROBANTE].astype(int).sum():,} Gs")

    logging.info("")

    ventas = clean_ventas(ventas)
    compras = clean_compras(compras)
    egresos = clean_egresos(egresos)

    total_compras = compras[COL_TOTAL_COMPROBANTE].astype(int).sum()
    total_ventas = ventas[COL_TOTAL_COMPROBANTE].astype(int).sum()
    total_egresos = egresos[COL_TOTAL_COMPROBANTE].astype(int).sum()
    total_diff = total_ventas - total_compras - total_egresos

    logging.info(f"Total ventas prestacion servicios profesionales: {total_ventas:,} Gs")
    logging.info("")

    total_gastos_salud_educ = np.int64(GASTOS_EXTERIOR_SALUD_EDUCACION)
    total_gastos_vehiculo_cada_3y = np.int64(GASTOS_VEHICULOS_CADA_3Y)

    egresos_activ_gravada = egresos[egresos[COL_RUC_EGRESOS].astype(str).isin(RUCS_EGRESOS_ACTIV_GRAVADA)]
    total_egresos_activ_gravada = egresos_activ_gravada[COL_TOTAL_COMPROBANTE].astype(int).sum() - total_gastos_vehiculo_cada_3y
    logging.info(f"Total egresos actividad gravada: {total_egresos_activ_gravada:,} Gs")

    compras_activ_gravada = compras[compras[COL_RUC].astype(str).isin(RUCS_EGRESOS_ACTIV_GRAVADA)]
    total_compras_activ_gravada = compras_activ_gravada[COL_TOTAL_COMPROBANTE].astype(int).sum()
    logging.info(f"Total compras actividad gravada: {total_compras_activ_gravada:,} Gs")

    logging.info("")

    egresos_estado_asoc = egresos[egresos[COL_RUC_EGRESOS].astype(str).isin(RUCS_ESTADO_ASOCIACIONES)]
    total_egresos_estado_asoc = egresos_estado_asoc[COL_TOTAL_COMPROBANTE].astype(int).sum()
    logging.info(f"Total egresos estado / asociaciones: {total_egresos_estado_asoc:,} Gs")

    compras_estado_asoc = compras[compras[COL_RUC].astype(str).isin(RUCS_ESTADO_ASOCIACIONES)]
    total_compras_estado_asoc = compras_estado_asoc[COL_TOTAL_COMPROBANTE].astype(int).sum()
    logging.info(f"Total compras estado / asociaciones: {total_compras_estado_asoc:,} Gs")

    logging.info("")

    total_gastos_estado_asoc = total_compras_estado_asoc + total_egresos_estado_asoc
    total_gastos_activ_gravada = total_egresos_activ_gravada + total_compras_activ_gravada

    total_gastos = total_compras + total_egresos
    total_gastos_familiares = total_gastos - total_gastos_activ_gravada - total_gastos_salud_educ - total_gastos_estado_asoc - total_gastos_vehiculo_cada_3y
    total_gastos_by_type = total_gastos_salud_educ + total_gastos_estado_asoc + total_gastos_activ_gravada + total_gastos_familiares + total_gastos_vehiculo_cada_3y

    logging.info(f"Total gastos en actividad gravada: {total_gastos_activ_gravada:,} Gs")
    logging.info(f"Total gastos familiares: {total_gastos_familiares:,} Gs")
    logging.info(f"Total gastos en el exterior salud / educacion: {total_gastos_salud_educ:,} Gs")
    logging.info(f"Total gastos en vehiculo cada 3y: {total_gastos_vehiculo_cada_3y:,} Gs")
    logging.info(f"Total gastos en estado y asociaciones: {total_gastos_estado_asoc:,} Gs")
    logging.info(f"Total gastos by type: {total_gastos_by_type:,} Gs")

    logging.info("")

    logging.info(f"Total compras: {total_compras:,} Gs")
    logging.info(f"Total egresos: {total_egresos:,} Gs")

    logging.info("")

    logging.info(f"Total gastos: {total_gastos:,} Gs")
    logging.info(f"Total ventas: {total_ventas:,} Gs")

    logging.info("")

    logging.info(f"Total difference: {total_diff:,} Gs")

    irp_8p = np.int64(np.ceil(min(max(total_diff.item(), 0), 50000000) * 0.08))
    irp_9p = np.int64(np.ceil(min(max(total_diff.item() - 50000000, 0), 100000000) * 0.09))
    irp_10p = np.int64(np.ceil(max(total_diff.item() - 150000000, 0) * 0.1))
    total_irp = irp_8p + irp_9p + irp_10p

    logging.info(f"IRP 8% (0-50M): {irp_8p:,} Gs")
    logging.info(f"IRP 9% (50-150M): {irp_9p:,} Gs")
    logging.info(f"IRP 10% (> 150M): {irp_10p:,} Gs")
    logging.info(f"Total IRP-RSP to pay: {total_irp:,} Gs")

    logging.info(f"Successfully generated IRP-RSP Form values")
