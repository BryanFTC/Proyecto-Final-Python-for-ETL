# ALUMNO: BRYAN TICONA
# OBS: PARA TESTEAR EL PROYECTO SE EMPLEO EL ARCHIVO deuda_df.ope QUE SE EMPLEO PARA EL
# LABORATORIO, SE LO RENOMBRO A file.ope.

# Importar librerías a utilizar
import pandas as pd
import sqlalchemy

# Definir variables a utilizar
input_folder = "server_inputs/"
output_folder = "server_outputs/"
input_file = "file.ope"
sqlite_db_path = "database/deudas_sbs.sqlite"

# EXTRACT///////////////////////////////////////////////////////////////////////////////
# Definir función EXTRACT


def extract():
    file_path = input_folder + input_file
    df = pd.read_csv(file_path)
    print('==> (EXTRACT) Los datos fueron extraidos')
    return df

# TRANSFORM/////////////////////////////////////////////////////////////////////////////
# Definir función TRANSFORM para deuda y cliente

# DEUDA/////////////////////////////////////////////////////////////////////////////////


def transform_deuda(df):

    df['Value'] = df['Field_1'].str[0]
    deuda_df = df[df['Value'] == '2'].reset_index(drop=True).copy()

    # Generando campos
    deuda_df['CodigoSBS'] = deuda_df['Field_1'].str[1:11]
    deuda_df['CodigoEmpresa'] = deuda_df['Field_1'].str[11:15]
    deuda_df['TipoCredito'] = deuda_df['Field_1'].str[16:17]
    deuda_df['Nivel2'] = deuda_df['Field_1'].str[18:19]
    deuda_df['Moneda'] = deuda_df['Field_1'].str[20]
    deuda_df['SubCodigoCuenta'] = deuda_df['Field_1'].str[21:31]
    deuda_df['Condicion'] = deuda_df['Field_1'].str[32:37]
    deuda_df['ValorSaldo'] = deuda_df['Field_1'].str[38:41]
    deuda_df['ClasificacionDeuda'] = deuda_df['Field_1'].str[42]
    deuda_df['CodigoCuenta'] = (
        deuda_df['Nivel2'] +
        deuda_df['Moneda'] +
        deuda_df['SubCodigoCuenta']
    )

    # Conversion de formatos
    deuda_df['CodigoSBS'] = deuda_df['CodigoSBS'].astype('float')

    # Eliminando columnas innecesarias
    deuda_df.drop('Field_1', axis=1, inplace=True)
    deuda_df.drop('Value', axis=1, inplace=True)
    deuda_df.drop('SubCodigoCuenta', axis=1, inplace=True)

    # Cabeceras del dataframe deuda (se renombro los campos 'CodigoSBS' —> 'Cod_SBS',
    # 'CodigoEmpresa'—> 'Cod_Emp','TipoCredito'—> 'Tip_Credit',
    # 'ValorSaldo'—> 'Val_Saldo', 'ClasificacionDeuda'—> 'Clasif_Deu',
    # 'CodigoCuenta'—> 'Cod_Cuenta')
    deuda_columns = {
        'CodigoSBS': 'Cod_SBS',
        'CodigoEmpresa': 'Cod_Emp',
        'TipoCredito': 'Tip_Credit',
        'ValorSaldo': 'Val_Saldo',
        'ClasificacionDeuda': 'Clasif_Deu',
        'CodigoCuenta': 'Cod_Cuenta'
    }
    deuda_df.rename(columns=deuda_columns, inplace=True)
    print('==> (TRANSFORM)(DEUDA) Los datos fueron transformados')
    return deuda_df

# CLIENTE///////////////////////////////////////////////////////////////////////////////


def transform_cliente(df):

    df['Value'] = df['Field_1'].str[0]
    cliente = df[df['Value'] == '1'].reset_index(drop=True).copy()

    # Cabeceras del dataframe cliente
    cliente_columns = {
        0: 'SBSCodigoCliente',
        1: 'SBSFechaReporte',
        2: 'SBSTipoDocumentoT',
        3: 'SBSRucCliente',
        4: 'SBSTipoDocumento',
        5: 'SBSNumeroDocumento',
        6: 'SBSTipoPer',
        7: 'SBSTipoEmpresa',
        8: 'SBSNumeroEntidad',
        9: 'SBSSalNor',
        10: 'SBSSalCPP',
        11: 'SBSSalDEF',
        12: 'SBSSalDUD',
        13: 'SBSSalAPER',
        14: 'SBSAPEPAT',
        15: 'SBSAPEMAT',
        16: 'SBSAPECAS',
        17: 'SBSNOMCLI',
        18: 'SBSNOMCLI2'
    }
    # Dividiendo el campo field_1 en 19 columnas:
    cliente_df = cliente['Field_1'].str.split('|', n=18, expand=True)

    # Renombrando las cabeceras del dataframe cliente
    cliente_df.rename(columns=cliente_columns, inplace=True)
    print('==> (TRANSFORM)(CLIENTE) Los datos fueron transformados')

    return cliente_df

# LOAD//////////////////////////////////////////////////////////////////////////////////
# Definir función Load para cliente y deuda

# CLIENTE///////////////////////////////////////////////////////////////////////////////


def load_cliente(cliente_df):

    # Guardar dataframe cliente en un archivo csv
    cliente_df.to_csv(output_folder + "cliente.csv", index=False, sep='|')
    print('==> (LOAD)(CLIENTE) El archivo cliente.csv fue creado satisfactoriamente')

# DEUDA/////////////////////////////////////////////////////////////////////////////////


def load_deuda(deuda_df):

    # Guardar dataframe deuda en un archivos csv
    deuda_df.to_csv(output_folder + "deuda.csv", index=False, sep='|')
    print('==> (LOAD)(DEUDA) El archivo deuda.csv fue creado satisfactoriamente')

    # Guardar dataframe deuda en la base de datos SQLite utilizando SQLAlchemy
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
        print('==> (LOAD) La tabla deudas_sbs fue creada satisfactoriamente')

    # Insertar los datos en la tabla deudas_sbs
    deuda_df.to_sql('deudas_sbs', db_engine, if_exists='replace', index=False)

    print(
        '==> (LOAD) Los datos fueron cargados satisfactoriamente en la tabla deudas_sbs'
    )


# Llamar a las funciones definidas EXTRACT, TRANSFORM y LOAD
if __name__ == "__main__":

    # Extracción
    df = extract()

    # Transformación
    deuda_df = transform_deuda(df)
    cliente_df = transform_cliente(df)

    # Carga
    load_deuda(deuda_df)
    load_cliente(cliente_df)
