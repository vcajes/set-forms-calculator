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
COL_RUC = "RUC / N? de Identificacion del Informado"
COL_RUC_EGRESOS = "RUC / N° de Identificación del Informado"
COL_CONDICION_OPERACION = "Condicion de la Operacion"
COL_TIPO_TODOS = ["VENTAS", "COMPRAS", "INGRESOS", "EGRESOS"]

RUCS_EGRESOS_FAMILIARES = [
    "80003296",  # ALIMENTOS ESPECIALES
    "80100678",  # SERVICIOS TURISTICOS DIVIAGGIO PARAGUAY S.A.
    "80016096",  # RETAIL SA
    "80016951",  # CADENA REAL SA
    "80006658",  # ALMACEN DE VIAJES-OPERADORES DE TURISMO Y REPRESENT. S.R.L.
    "80093649",  # CONSORCIO DE COPROPIETARIOS DEL CONDOMINIO RESIDENCIAL
    "80106771",  # ADMA SOCIEDAD ANONIMA
    "80022202",  # ALIMENTOS Y SERVICIOS SRL
    "80092105",  # BFJ S.A.
    "80089760",  # NAILS EXPRESS BY ROSSIE S.A.
    "80078953",  # WALTER S.A.
    "80002014",  # Manufactura de Pilar S.A.
    "80025405",  # LA VIENESA SA
    "80082790",  # VIGOR SA
    "80025958",  # PLAZA OFERTA S.A.
    "80018381",  # RED UTS PARAGUAY SA
    "80072380",  # MARKETPLACE S.A.
    "80107312",  # KARU PORA S.A.
    "3212838",   # BRUNO TEODORO BRUSQUETTI CABRERA
    "80000402",  # AGRO INDUSTRIAL GUARAPI S.A.
    "80102209",  # MOT SOCIEDAD ANONIMA
    "80122103",  # PUNTA SPORT SOCIEDAD ANONIMA
    "80095560",  # PROSPERAR PARAGUAY S.A.
    "80092500",  # QSR S.A.
    "4932508",   # WILFRIED ROSSEL
    "80100678",  # SERVICIOS TURISTICOS DIVIAGGIO PARAGUAY S.A.
    "80027920",  # SOUTH FOOD SA
    "80077406",  # BIGGIE S.A.
    "80106641",  # COI COI SOCIEDAD ANONIMA
    "8609876",   # CINTHIA OBERMANN
    "80048348",  # VELUTE S.R.L.
    "80122730",  # SEÑOR PARRILLA E.A.S.
    "80001513",  # NUEVA AMERICANA SA
]

RUCS_ESTADO_ASOCIACIONES = [
    "80004239",  # MOPC
    "80027621",  # CLUB CENTENARIO
    "80029733",  # CLUB OLIMPIA
    "80031086",  # CLUB NAUTICO
]

GASTOS_EXTERIOR_SALUD_EDUCACION = 884_167 + 922_692 + 1_024_807 + 1_155_490 + 140_166


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
    # remove rows CREDITO as those cannot be deducted without an EGRESOS entry
    compras = compras[~compras[COL_CONDICION_OPERACION].astype(str).isin(["CREDITO"])]

    # ensure that the rows are either for IRP deduction or not
    if {COL_IMPUTA_IRP, COL_NO_IMPUTAR}.issubset(compras.columns):
        assert np.where((compras[COL_IMPUTA_IRP] == "NO") & (compras[COL_NO_IMPUTAR] == "NO"))[0].size == 0
        assert np.where((compras[COL_IMPUTA_IRP] == "SI") & (compras[COL_NO_IMPUTAR] == "SI"))[0].size == 0

    # remove rows that will not be deducted for IRP
    compras = compras[~compras[COL_IMPUTA_IRP].astype(str).isin(["NO"])]

    # ensure restrictions are met
    assert np.where((compras[COL_CONDICION_OPERACION] == "CREDITO"))[0].size == 0
    assert np.where((compras[COL_IMPUTA_IRP] == "NO"))[0].size == 0

    # keep only columns we need
    compras = compras[[COL_TIPO_REGISTRO, COL_RUC, COL_NUMERO_COMPROBANTE, COL_TOTAL_COMPROBANTE]]

    compras[COL_RUC] = compras[COL_RUC].astype(str).apply(lambda x: x.replace(".0", ""))

    # convert total amount to int
    compras[COL_TOTAL_COMPROBANTE] = compras[COL_TOTAL_COMPROBANTE].fillna(0).astype(int)

    return compras


def clean_ventas(ventas):
    # remove rows CREDITO as those cannot be added with there is INGRESOS associated
    ventas = ventas[~ventas[COL_CONDICION_OPERACION].astype(str).isin(["CREDITO"])]

    # remove rows that will not be deducted for IRP
    ventas = ventas[~ventas[COL_IMPUTA_IRP].astype(str).isin(["NO"])]

    # ensure restrictions are met
    assert np.where((ventas[COL_CONDICION_OPERACION] == "CREDITO"))[0].size == 0
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

    ventas = clean_ventas(ventas)
    compras = clean_compras(compras)
    egresos = clean_egresos(egresos)

    total_compras = compras[COL_TOTAL_COMPROBANTE].astype(int).sum()
    total_ventas = ventas[COL_TOTAL_COMPROBANTE].astype(int).sum()
    total_egresos = egresos[COL_TOTAL_COMPROBANTE].astype(int).sum()
    total_diff = total_ventas - total_compras - total_egresos

    logging.info("")

    egresos_familiares = egresos[egresos[COL_RUC_EGRESOS].astype(str).isin(RUCS_EGRESOS_FAMILIARES)]
    total_egresos_familiares = egresos_familiares[COL_TOTAL_COMPROBANTE].astype(int).sum()
    logging.info(f"Total egresos familiares: {total_egresos_familiares} Gs")

    compras_familiares = compras[compras[COL_RUC].astype(str).isin(RUCS_EGRESOS_FAMILIARES)]
    total_compras_familiares = compras_familiares[COL_TOTAL_COMPROBANTE].astype(int).sum()
    logging.info(f"Total compras familiares: {total_compras_familiares} Gs")

    logging.info("")

    egresos_estado_asoc = egresos[egresos[COL_RUC_EGRESOS].astype(str).isin(RUCS_ESTADO_ASOCIACIONES)]
    total_egresos_estado_asoc = egresos_estado_asoc[COL_TOTAL_COMPROBANTE].astype(int).sum()
    logging.info(f"Total egresos estado / asociaciones: {total_egresos_estado_asoc} Gs")

    compras_estado_asoc = compras[compras[COL_RUC].astype(str).isin(RUCS_ESTADO_ASOCIACIONES)]
    total_compras_estado_asoc = compras_estado_asoc[COL_TOTAL_COMPROBANTE].astype(int).sum()
    logging.info(f"Total compras estado / asociaciones: {total_compras_estado_asoc} Gs")

    logging.info("")

    total_gastos = total_compras + total_egresos
    total_gastos_familiares = total_egresos_familiares + total_compras_familiares
    total_gastos_salud_educ = GASTOS_EXTERIOR_SALUD_EDUCACION
    total_gastos_estado_asoc = total_compras_estado_asoc + total_egresos_estado_asoc
    total_gastos_activ_gravada = total_gastos - total_gastos_familiares - total_gastos_salud_educ - total_gastos_estado_asoc

    logging.info(f"Total gastos familiares: {total_gastos_familiares} Gs")
    logging.info(f"Total gastos en el exterior salud / educacion: {total_gastos_salud_educ} Gs")
    logging.info(f"Total gastos en estado y asociaciones: {total_gastos_estado_asoc} Gs")
    logging.info(f"Total gastos en actividad gravada: {total_gastos_activ_gravada} Gs")

    logging.info("")

    logging.info(f"Total compras: {total_compras} Gs")
    logging.info(f"Total egresos: {total_egresos} Gs")

    logging.info("")

    logging.info(f"Total gastos: {total_gastos} Gs")
    logging.info(f"Total ventas: {total_ventas} Gs")

    logging.info("")

    logging.info(f"Total difference: {total_diff} Gs")
    logging.info(f"Estimated IRP-RSP to pay: {int(total_diff * 0.1)} Gs")

    logging.info(f"Successfully generated IRP-RSP Form values")
