# ALUMNO: BRYAN TICONA
# OBS: PARA TESTEAR EL PROYECTO SE EMPLEO EL ARCHIVO data.ope QUE SE EMPLEO PARA EL
# LABORATORIO, SE LO RENOMBRO A file.ope.
# ESTE ARCHIVO NO POSEE LOS DATOS NECESARIOS PARA EL PROYECTO, POR LO CUAL EL RESULTADO
# ES UNA TABLA Y DOS ARCHIVOS CSV SIN DATOS PERO CON LOS ENCABEZADOS SOLICITADOS.

# Importar librerías a utilizar
import pandas as pd
import sqlalchemy

# Definir variables a utilizar
input_folder = "server_inputs/"
output_folder = "server_outputs/"
input_file = "file.ope"
sqlite_db_path = "database/deudas_sbs.sqlite"
# file_path = input_folder + input_file

# EXTRACT///////////////////////////////////////////////////////////////////////////////
# Definir función EXTRACT


def extract():
    file_path = input_folder + input_file
    data = pd.read_csv(file_path)
    print('==> (Extract) Los datos fueron extraidos')
    return data

# TRANSFORM/////////////////////////////////////////////////////////////////////////////
# Definir función TRANSFORM


def transform(data):
    cliente_data = []
    deuda_data = []

    for line in data:
        if line.startswith('1'):
            fields = line.strip().split('|')
            cliente_data.append(fields)
        elif line.startswith('2'):
            codigo_sbs = line[0:10]
            codigo_empresa = line[10:15]
            tipo_credito = line[15:17]
            nivel2 = line[17:19]
            moneda = line[19]
            sub_codigo_cuenta = line[20:31]
            condicion = line[31:37]
            valor_saldo = line[37:41]
            clasificacion_deuda = line[41]
            codigo_cuenta = nivel2 + moneda + sub_codigo_cuenta

            deuda_data.append([
                codigo_sbs, codigo_empresa, tipo_credito, nivel2, moneda,
                condicion, valor_saldo, clasificacion_deuda, codigo_cuenta
            ])

    # Cabeceras del dataframe cliente
    cliente_columns = [
        'SBSCodigoCliente', 'SBSFechaReporte', 'SBSTipoDocumentoT', 'SBSRucCliente',
        'SBSTipoDocumento', 'SBSNumeroDocumento', 'SBSTipoPer', 'SBSTipoEmpresa',
        'SBSNumeroEntidad', 'SBSSalNor', 'SBSSalCPP', 'SBSSalDEF', 'SBSSalDUD',
        'SBSSalAPER', 'SBSAPEPAT', 'SBSAPEMAT', 'SBSAPECAS', 'SBSNOMCLI', 'SBSNOMCLI2'
    ]

    # DataFrame cliente:
    cliente_df = pd.DataFrame(cliente_data, columns=cliente_columns)

    # Cabeceras del dataframe deuda (se renombro los campos ‘CodigoSBS' —> 'Cod_SBS',
    # 'CodigoEmpresa'—> 'Cod_Emp','TipoCredito'—> 'Tip_Credit',
    # 'ValorSaldo'—> 'Val_Saldo', 'ClasificacionDeuda'—> 'Clasif_Deu',
    # 'CodigoCuenta'—> 'Cod_Cuenta')

    deuda_columns = [
        'Cod_SBS', 'Cod_Emp', 'Tip_Credit', 'Nivel2', 'Moneda', 'Condicion',
        'Val_Saldo', 'Clasif_Deu', 'Cod_Cuenta'
    ]

    # DataFrame deuda
    deuda_df = pd.DataFrame(deuda_data, columns=deuda_columns)

    print('==> (Transform) Los datos fueron transformados')

    return cliente_df, deuda_df

# LOAD//////////////////////////////////////////////////////////////////////////////////
# Definir función Load


def load(cliente_df, deuda_df):

    # Guardar DataFrames en archivos CSV
    cliente_df.to_csv(output_folder + "cliente.csv", index=False, sep='|')
    deuda_df.to_csv(output_folder + "deuda.csv", index=False, sep='|')

    # Eliminar campos no deseados del DataFrame deuda_df
    # ('Field_1', 'Value', 'SubCodigoCuenta')
    # deuda_df = deuda_df.drop(columns=['Field_1', 'Value', 'SubCodigoCuenta'])
    # OBS: EN EL ARCHIVO .OPE QUE SE ESTA UTILIZANDO PARA EL TEST NO EXISTEN LOS CAMPOS
    # QUE SE DESEA ELIMINAR POR ESA RAZON SE COMENTO ESTA PARTE DEL CODIGO PARA NO
    # RECIBIR ERRORES

    # Guardar Deudas en la base de datos SQLite utilizando SQLAlchemy
    db_engine = sqlalchemy.create_engine('sqlite:///' + sqlite_db_path)

    # Crear la tabla deudas_sbs
    with db_engine.connect() as con:
        sql_query = '''
            CREATE TABLE IF NOT EXISTS deudas_sbs (
                Cod_SBS VARCHAR(200),
                Cod_Emp VARCHAR(200),
                Tip_Credit VARCHAR(200),
                Nivel2 VARCHAR(200),
                Moneda VARCHAR(200),
                Condicion VARCHAR(200),
                Val_Saldo VARCHAR(200),
                Clasif_Deu VARCHAR(200),
                Cod_Cuenta VARCHAR(200)
            )
        '''
        sql_query = sqlalchemy.sql.text(sql_query)
        con.execute(sql_query)
        print('==> (LOAD) La tabla fue creada satisfactoriamente')

    # Insertar los datos en la tabla deudas_sbs
    deuda_df.to_sql('deudas_sbs', db_engine, if_exists='replace', index=False)

    print('==> (LOAD) Los datos fueron cargados satisfactoriamente')


# Llamar a las funciones definidas EXTRACT, TRANSFORM y LOAD
if __name__ == "__main__":

    # Extracción
    data = extract()

    # Transformación
    cliente_df, deuda_df = transform(data)

    # Carga
    load(cliente_df, deuda_df)
